package k8s

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestPodStatusHintImagePull(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			ContainerStatuses: []corev1.ContainerStatus{{
				Ready: false,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason:  "ErrImagePull",
						Message: "not found",
					},
				},
			}},
		},
	}
	reason, msg := PodStatusHint(pod)
	if reason != "ErrImagePull" || msg != "not found" {
		t.Fatalf("got %q / %q", reason, msg)
	}
}

func TestPickWorstStatus(t *testing.T) {
	r, _ := PickWorstStatus("Pending", "", "CrashLoopBackOff", "exit 1")
	if r != "CrashLoopBackOff" {
		t.Fatalf("got %q", r)
	}
}
