import json
import pathlib
import sys

root = pathlib.Path(__file__).resolve().parent.parent
out = pathlib.Path(sys.argv[1]).resolve()
out.mkdir(parents=True, exist_ok=True)


def overlay(name, replacements):
    paths = {}
    for relative, source in replacements.items():
        target = out / (name + '-' + relative.replace('/', '_'))
        target.write_text(source)
        paths[str(root / relative)] = str(target)
    (out / (name + '.json')).write_text(json.dumps({'Replace': paths}))


admission = (root / 'internal/admission/admission.go').read_text()
overlay('limiter-candidate', {'internal/admission/admission.go': admission.replace(
    'now.Sub(l.swept) > IdleTTL || len(l.entries) >= MaxEntries',
    'now.Sub(l.swept) > IdleTTL')})

server = (root / 'internal/relay/server.go').read_text()
overlay('buffer64', {'internal/relay/server.go': server.replace(
    'WriteBufferSize:  16 * 1024', 'WriteBufferSize:  64 * 1024')})

control = (root / 'internal/relay/control.go').read_text()
needle = 'ctrl.q.push(textFrame(map[string]string{"type": "registered", "endpointId": id}))\n\ts.mu.Lock()'
assert needle in control
hook = '''ctrl.q.push(textFrame(map[string]string{"type": "registered", "endpointId": id}))
    if hook := spikeBeforeRegistrationLock.Load(); hook != nil { (*hook)(h) }
    s.mu.Lock()'''
instrumented = control.replace(needle, hook)
instrumented += '\nvar spikeBeforeRegistrationLock atomic.Pointer[func(*host)]\n'
overlay('registration-baseline', {'internal/relay/control.go': instrumented})
fixed = instrumented.replace('version: s.clock.Next()', 'version: 0')
fixed = fixed.replace(hook, hook + '\n    h.version = s.clock.Next()')

cluster = (root / 'internal/relay/cluster.go').read_text()
eviction = cluster.replace('delete(s.hosts, h.id)\n\ts.mu.Unlock()',
    'delete(s.hosts, h.id)\n    s.publishLocked(directory.Message{Type: directory.TypeDel, Registration: s.registration(h)})\n\ts.mu.Unlock()')
overlay('registration-candidate', {
    'internal/relay/control.go': fixed,
    'internal/relay/cluster.go': eviction,
})

fencing = cluster.replace('func (s *Server) RunDirectory(ctx context.Context) {',
    'func (s *Server) RunDirectory(ctx context.Context) {\n    go s.spikeDirectoryFence(ctx)')
fencing += '''
func (s *Server) spikeDirectoryFence(ctx context.Context) {
    tick := time.NewTicker(50*time.Millisecond)
    defer tick.Stop()
    lastReady := time.Now()
    for {
        select {
        case <-ctx.Done(): return
        case <-tick.C:
            s.mu.Lock()
            if len(s.dirStreams)>0 { lastReady=time.Now(); s.mu.Unlock(); continue }
            if time.Since(lastReady)<300*time.Millisecond { s.mu.Unlock(); continue }
            hosts:=make([]*host,0,len(s.hosts))
            for id,h:=range s.hosts { delete(s.hosts,id); h.detached=true; hosts=append(hosts,h) }
            s.mu.Unlock()
            for _,h:=range hosts { h.ctrl.q.finish(closeMsg{websocket.CloseGoingAway,"relay draining"},true) }
        }
    }
}
'''
fenced_server = server.replace(
    'func (s *Server) admit(w http.ResponseWriter, r *http.Request, duringDrain bool) bool {',
    '''func (s *Server) admit(w http.ResponseWriter, r *http.Request, duringDrain bool) bool {
    if s.cfg.Clustered() && !duringDrain {
        s.mu.Lock(); unavailable:=len(s.dirStreams)==0; s.mu.Unlock()
        if unavailable { s.reject(w,r,http.StatusServiceUnavailable,"directory unavailable"); return false }
    }''')
overlay('directory-candidate', {
    'internal/relay/cluster.go': fencing,
    'internal/relay/server.go': fenced_server,
})

router_directory = (root / 'cmd/router/directory.go').read_text()
start = router_directory.index('func (rt *router) evict(')
end = router_directory.index('func (rt *router) refresh(', start)
batched = '''func (rt *router) evict(evs []directory.Eviction) {
    groups:=map[string][]directory.Eviction{}
    for _,ev:=range evs { groups[ev.NodeID]=append(groups[ev.NodeID],ev) }
    for id,group:=range groups {
        rt.mu.Lock(); st:=rt.streams[id]; rt.mu.Unlock()
        if st==nil { continue }
        for start:=0;start<len(group);start+=128 {
            chunk:=group[start:min(start+128,len(group))]
            regs:=make([]directory.Registration,len(chunk))
            var clock uint64
            for i,ev:=range chunk { regs[i]=ev.Registration;clock=max(clock,ev.Winner) }
            if st.send(directory.Message{Type:directory.TypeEvict,Registrations:regs,Clock:clock}) {
                rt.evictions.Add(int64(len(chunk)))
            }
        }
    }
}

'''
batch_node = cluster.replace('s.evict(m)', '''if m.Registration!=nil { s.evict(m) } else {
    for _,reg:=range m.Registrations { s.evict(directory.Message{Clock:m.Clock,Registration:&reg}) }
}''')
overlay('eviction-batch', {
    'cmd/router/directory.go': router_directory[:start] + batched + router_directory[end:],
    'internal/relay/cluster.go': batch_node,
})
