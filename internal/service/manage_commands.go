// ManageService/AgentInfo/HeapDumpService/DumpService management handlers:
// Copyright 2015 Scouter authors, Apache-2.0. Independent implementation of
// pinned source dee47fb5.
package service

import (
	"context"
	"fmt"
	"log"
	"path"
	"strings"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// manageLogf writes the one-line audit record of every management command
// (Java Logger.println in ManageService; AgentCall RequestLogger). Tests
// replace it; it never receives secrets or request bodies.
var manageLogf = log.Printf

// manageMaxItems bounds request lists (objHash, file) like the other handlers'
// 4096-item response limit.
const manageMaxItems = 4096

// manageMaxDeletePaths matches storage's maxDeletePaths so a longer list is
// refused here (ErrLimit) instead of after the deleter rejects it.
const manageMaxDeletePaths = 1024

// databaseLockFile is DBCtr.scala:40's server lock below the database root.
const databaseLockFile = "lock.dat"

// DatabaseDeleter is optional and owned by storage. Each path is root-relative,
// slash-separated and already cleaned (no leading separator, no "." or ".."
// component, never the root or lock.dat). The implementation anchors every
// access on the database root, refuses symlinks and deletes recursively; a
// missing path is success (ManageService.deleteFiles returns on !exists).
type DatabaseDeleter interface {
	DeleteDatabasePaths(ctx context.Context, paths []string) error
}

// ManageCommands lists the remote control, DB delete, object removal and Agent
// mutation relays (ManageService, AgentInfo, HeapDumpService, DumpService,
// ThreadList, StackAnalyzerService, ConfigureService, CubridService,
// DatabaseService) handled by HandleManageCommand. CHECK_JOB is listed here so
// that this handler, which owns the RemoteControlManager queue, runs before
// the metadata no-job answer.
func ManageCommands() []string {
	commands := []string{"REMOTE_CONTROL", "REMOTE_CONTROL_ALL", "CHECK_JOB", "SERVER_DB_DELETE", "OBJECT_REMOVE", "OBJECT_REMOVE_IN_MEMORY", "OBJECT_REMOVE_INACTIVE"}
	return append(commands, AgentMutationCommands()...)
}

// HandleManageCommand recognizes its commands before reading the body and leaves
// the stream intact for unknown commands. Every command requires a session;
// Java group policy (AllowHeapDump, AllowConfigure, ...) is not evaluated, so
// each request is logged instead.
func (s *Service) HandleManageCommand(ctx context.Context, r transport.ClientRequest, stream *transport.Stream) (bool, error) {
	recognized := false
	for _, command := range ManageCommands() {
		if command == r.Command {
			recognized = true
			break
		}
	}
	if !recognized {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if !r.Authenticated {
		return true, transport.ErrUnauthorized
	}
	var err error
	switch r.Command {
	case "REMOTE_CONTROL", "REMOTE_CONTROL_ALL", "CHECK_JOB":
		err = s.remoteControlCommand(ctx, r, stream)
	case "SERVER_DB_DELETE":
		err = s.serverDatabaseDelete(ctx, r, stream)
	case "OBJECT_REMOVE", "OBJECT_REMOVE_IN_MEMORY", "OBJECT_REMOVE_INACTIVE":
		err = s.objectRemoveCommand(ctx, r, stream)
	default:
		err = s.agentMutation(ctx, r, stream)
	}
	// management-commands.md §9-10: Java swallows handler exceptions and
	// answers no items; here the failure is logged explicitly and the
	// connection follows the existing error policy (no further items).
	if err != nil && ctx.Err() == nil {
		manageLogf("[%s] rejected: %v session=%d peer=%v", r.Command, err, r.Session, r.Peer)
	}
	return true, err
}

func (s *Service) databaseDeleter() (DatabaseDeleter, bool) {
	if d, ok := s.backend.(DatabaseDeleter); ok {
		return d, true
	}
	if d, ok := s.config.Metadata.(DatabaseDeleter); ok {
		return d, true
	}
	return nil, false
}

// serverDatabaseDelete is ManageService.deleteDbFiles (ManageService.java:57-73):
// MapPack{file: List[Text]} → delete rootPath+filename recursively → MapPack
// {size: count}; an absent list answers an empty MapPack. Java concatenates
// the strings without validating the name. Here every name must be a SERVER_DB_LIST style
// "/<relative>" path inside the root and the lock file is protected; the
// response shape is unchanged.
func (s *Service) serverDatabaseDelete(ctx context.Context, r transport.ClientRequest, stream *transport.Stream) error {
	p, err := readMap(stream)
	if err != nil {
		return err
	}
	files := lookup(p, "file")
	if files.Type == protocol.ValueNull {
		return stream.ResponsePack(newMap())
	}
	if files.Type != protocol.ValueList {
		return badParam("file")
	}
	if len(files.Values) > manageMaxDeletePaths {
		return transport.ErrLimit
	}
	deleter, ok := s.databaseDeleter()
	if !ok {
		return fmt.Errorf("%w: SERVER_DB_DELETE requires a database deleter", transport.ErrUnsupported)
	}
	paths := make([]string, 0, len(files.Values))
	for _, v := range files.Values {
		// ListValue.getString would stringify non-Text values; reject instead.
		if v.Type != protocol.ValueText {
			return badParam("file")
		}
		rel, err := databaseRelativePath(v.Text)
		if err != nil {
			return err
		}
		paths = append(paths, rel)
	}
	manageLogf("[SERVER_DB_DELETE] %d path(s) %q session=%d peer=%v", len(paths), paths, r.Session, r.Peer)
	if len(paths) > 0 {
		if err := deleter.DeleteDatabasePaths(ctx, paths); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.ResponsePack(newMap(entry("size", decimal(int64(len(files.Values))))))
}

// databaseRelativePath validates one SERVER_DB_DELETE name. SERVER_DB_LIST
// reports "/<dir>" (absolute path minus the root prefix), which is the only
// accepted form: the leading separator is removed, the rest is cleaned and
// must stay strictly below the root.
func databaseRelativePath(name string) (string, error) {
	if name == "" || !strings.HasPrefix(name, "/") || strings.ContainsAny(name, "\x00\\") {
		return "", badParam("file")
	}
	rel := path.Clean(strings.TrimPrefix(name, "/"))
	if rel == "." || rel == ".." || strings.HasPrefix(rel, "../") || strings.HasPrefix(rel, "/") || rel == databaseLockFile {
		return "", badParam("file")
	}
	return rel, nil
}
