package service

import (
	"context"

	"github.com/snowvil/scouter-go/internal/transport"
)

// AgentMutationCommands returns the 21 Client→Agent relays whose Agent handler
// changes JVM/process state (heap dump, profiles, System.gc, thread control,
// stack sampling, dump triggers, configuration rewrite, class redefinition,
// DB session control). The server-side handlers are identical to the read
// relays: MapPack body, objHash target, final MapPack answered when present.
func AgentMutationCommands() []string {
	return []string{
		"OBJECT_CALL_HEAP_DUMP", "OBJECT_CALL_BLOCK_PROFILE", "OBJECT_CALL_MUTEX_PROFILE", "OBJECT_CALL_CPU_PROFILE", "OBJECT_DELETE_HEAP_DUMP",
		"OBJECT_RESET_CACHE", "OBJECT_SYSTEM_GC", "OBJECT_THREAD_CONTROL", "PSTACK_ON",
		"TRIGGER_ACTIVE_SERVICE_LIST", "TRIGGER_HEAPHISTO", "TRIGGER_THREAD_DUMP", "TRIGGER_THREAD_LIST", "TRIGGER_BLOCK_PROFILE", "TRIGGER_MUTEX_PROFILE",
		"SET_CONFIGURE_WAS", "REDEFINE_CLASSES", "CUBRID_SET_ALERT_CONFIGURE", "DB_KILL_PROCESS", "SET_QUERY_INTERVAL", "USE_DATABASE",
	}
}

// agentMutationSilent marks the two handlers that discard the Agent answer:
// AgentInfo.resetAgentCache (AgentInfo.scala:147-156) and DumpService.systemGc
// (DumpService.scala:133-139).
func agentMutationSilent(command string) bool {
	return command == "OBJECT_RESET_CACHE" || command == "OBJECT_SYSTEM_GC"
}

// agentMutation relays one mutation like agentReadQuery (last MapPack or no
// item, offline or failed Agent → no item, request-wide error → failure)
// after logging the command, target and session; the Java group policy check
// is not evaluated.
func (s *Service) agentMutation(ctx context.Context, r transport.ClientRequest, stream *transport.Stream) error {
	p, err := readMap(stream)
	if err != nil {
		return err
	}
	hash, err := getInt(p, "objHash")
	if err != nil {
		return err
	}
	manageLogf("[%s] objHash=%d session=%d peer=%v", r.Command, hash, r.Session, r.Peer)
	if !agentMutationSilent(r.Command) {
		return s.agentReadQuery(ctx, r.Command, p, stream)
	}
	b, ok := s.backend.(AgentBackend)
	if !ok {
		return transport.ErrUnsupported
	}
	results, err := awaitAgent(ctx, stream, b, hash, r.Command, p)
	if err != nil {
		if !skippableAgentError(ctx, err) {
			return err
		}
		// AgentInfo.scala:147-156 / DumpService.scala:133-139 ignore
		// AgentCall's result, null after an agent failure included.
		noteAgentRelaySkip(r.Command, hash, false, err)
		results = nil
	}
	if len(results) > manageMaxItems {
		return transport.ErrLimit
	}
	return ctx.Err()
}
