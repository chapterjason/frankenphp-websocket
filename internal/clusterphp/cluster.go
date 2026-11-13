package clusterphp

// #cgo linux CFLAGS: -D_GNU_SOURCE
// #include <Zend/zend_types.h>
//
// // Each PHP thread runs on its own OS thread, so this is per PHP thread.
// static __thread long cluster_last_error_code;
//
// static void cluster_set_last_error(long code) { cluster_last_error_code = code; }
// static long cluster_get_last_error(void) { return cluster_last_error_code; }
import "C"
import (
	"errors"
	"maps"
	"slices"
	"time"
	"unsafe"

	"github.com/chapterjason/frankenphp-websocket/internal/cluster"
	"github.com/dunglas/frankenphp"
)

// export_php:const
const CLUSTER_ERROR_NONE = 0

// export_php:const
const CLUSTER_ERROR_NOT_JOINED = 1

// export_php:const
const CLUSTER_ERROR_INVALID_ARGUMENT = 2

// export_php:const
const CLUSTER_ERROR_BUS_FAILED = 3

// export_php:const
const CLUSTER_ERROR_TIMEOUT = 4

// export_php:const
const CLUSTER_ERROR_NO_RECEIVER = 5

func setLastError(code int64) {
	C.cluster_set_last_error(C.long(code))
}

// succeeded records the code of err as the last error and reports whether
// there was none.
func succeeded(err error) bool {
	code := errorCode(err)
	setLastError(code)

	return code == CLUSTER_ERROR_NONE
}

func errorCode(err error) int64 {
	switch {
	case err == nil:
		return CLUSTER_ERROR_NONE
	case errors.Is(err, cluster.ErrNotJoined):
		return CLUSTER_ERROR_NOT_JOINED
	case errors.Is(err, cluster.ErrTimeout):
		return CLUSTER_ERROR_TIMEOUT
	case errors.Is(err, cluster.ErrNoReceiver):
		return CLUSTER_ERROR_NO_RECEIVER
	default:
		return CLUSTER_ERROR_BUS_FAILED
	}
}

func goString(s *C.zend_string) string {
	return frankenphp.GoString(unsafe.Pointer(s))
}

// clusterSelf returns the instance ID of this server in the cluster.
//
// export_php:function cluster_self(): string
func clusterSelf() unsafe.Pointer {
	setLastError(CLUSTER_ERROR_NONE)

	return frankenphp.PHPString(cluster.Local.ID(), false)
}

// clusterMembers lists the instance IDs of the other members known to be
// alive.
//
// export_php:function cluster_members(): array
func clusterMembers() unsafe.Pointer {
	setLastError(CLUSTER_ERROR_NONE)

	return frankenphp.PHPPackedArray(cluster.Local.Members())
}

// clusterMemberInfo describes the other members known to be alive: by
// instance ID, their role and whether they receive what cluster_send(),
// cluster_publish() and cluster_request() send them.
//
// export_php:function cluster_member_info(): array
func clusterMemberInfo() unsafe.Pointer {
	setLastError(CLUSTER_ERROR_NONE)

	members := cluster.Local.MembersInfo()
	info := frankenphp.AssociativeArray[any]{Map: make(map[string]any, len(members)), Order: slices.Sorted(maps.Keys(members))}
	for id, member := range members {
		info.Map[id] = frankenphp.AssociativeArray[any]{
			Map:   map[string]any{"role": member.Role.String(), "receives": member.Capabilities.Has(cluster.Receives)},
			Order: []string{"role", "receives"},
		}
	}

	return frankenphp.PHPAssociativeArray(info)
}

// clusterSend sends data to the worker of one member. It is fire and forget:
// true means the message is queued for that member, or that it is gone; a
// member that announced it does not receive gets nothing, false with
// CLUSTER_ERROR_NO_RECEIVER.
//
// export_php:function cluster_send(string $instance, string $data): bool
func clusterSend(instance *C.zend_string, data *C.zend_string) bool {
	to := goString(instance)
	if to == "" {
		setLastError(CLUSTER_ERROR_INVALID_ARGUMENT)

		return false
	}

	return succeeded(cluster.Local.Send(to, cluster.MessagesChannel, []byte(goString(data))))
}

// clusterPublish sends data to the workers of all other members that announced
// they receive, and returns for how many it was queued.
//
// export_php:function cluster_publish(string $data): int
func clusterPublish(data *C.zend_string) int64 {
	queued, err := cluster.Local.Publish(cluster.MessagesChannel, []byte(goString(data)))
	setLastError(errorCode(err))

	return int64(queued)
}

// clusterRequest sends data to the worker of one member and returns its
// answer; when there is none, an empty string with the last error set, as
// FrankenPHP's generator turns a null return into one. timeout is in
// milliseconds; 0 waits the query_timeout of cluster member.
//
// export_php:function cluster_request(string $instance, string $data, int $timeout = 0): string
func clusterRequest(instance *C.zend_string, data *C.zend_string, timeout int64) unsafe.Pointer {
	to := goString(instance)
	if to == "" || timeout < 0 {
		setLastError(CLUSTER_ERROR_INVALID_ARGUMENT)

		return frankenphp.PHPString("", false)
	}

	answer, err := cluster.Local.Request(to, cluster.MessagesChannel, []byte(goString(data)), time.Duration(timeout)*time.Millisecond)
	if !succeeded(err) {
		return frankenphp.PHPString("", false)
	}

	return frankenphp.PHPString(string(answer), false)
}

// clusterLastError returns the CLUSTER_ERROR_* code of the last cluster_*
// call made by this PHP thread.
//
// export_php:function cluster_last_error(): int
func clusterLastError() int64 {
	return int64(C.cluster_get_last_error())
}
