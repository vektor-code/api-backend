package traceinvest

import (
	"fmt"
	"net/url"
	"strconv"
)

// httpProbeArgv builds a policy-controlled probe. It never changes the app
// process. If wget/curl/python are missing it may install wget with the image's
// own package manager, run one GET, then uninstall wget in a trap so a timeout
// still cleans up. It does not upgrade packages, restart the container, or
// write into application directories.
func httpProbeArgv(raw string) ([]string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("unsupported scheme")
	}
	if u.Host == "" || u.User != nil {
		return nil, fmt.Errorf("invalid host")
	}
	cleaned := u.Scheme + "://" + u.Host + u.EscapedPath()
	if u.RawQuery != "" {
		cleaned += "?" + u.RawQuery
	}
	quoted := strconv.Quote(cleaned)
	return []string{"/bin/sh", "-c", fmt.Sprintf(httpProbeScript, quoted, quoted, quoted)}, nil
}

// httpProbeScript is a single exec. %s is a Go-quoted URL used as a shell
// assignment and as a Python string literal.
const httpProbeScript = `URL=%s
MARKER=/tmp/.crnet-apm-probe.$$
TMPBIN=/tmp/crnet-apm-httpprobe.$$
set +e

cleanup() {
  if [ -f "$MARKER" ]; then
    kind=$(cat "$MARKER" 2>/dev/null)
    case "$kind" in
      apk-wget)
        apk del --quiet wget >/dev/null 2>&1 || true
        ;;
      apt-wget)
        DEBIAN_FRONTEND=noninteractive apt-get remove -y -qq wget >/dev/null 2>&1 || true
        ;;
      microdnf-wget)
        microdnf remove -y wget >/dev/null 2>&1 || true
        ;;
      yum-wget)
        yum remove -y wget >/dev/null 2>&1 || true
        ;;
    esac
    rm -f "$MARKER"
  fi
  rm -f "$TMPBIN"
}
trap cleanup EXIT INT TERM HUP

have_client() {
  command -v wget >/dev/null 2>&1 && return 0
  command -v curl >/dev/null 2>&1 && return 0
  command -v python3 >/dev/null 2>&1 && return 0
  command -v python >/dev/null 2>&1 && return 0
  command -v busybox >/dev/null 2>&1 && return 0
  return 1
}

run_probe() {
  if command -v wget >/dev/null 2>&1; then
    wget -S -O /dev/null -T 2 --tries=1 "$URL"
    return $?
  fi
  if command -v curl >/dev/null 2>&1; then
    curl -sS -o /dev/null -D - --connect-timeout 2 --max-time 5 "$URL"
    return $?
  fi
  if command -v python3 >/dev/null 2>&1; then
    python3 -c 'import urllib.request,urllib.error
u=%s
try:
 r=urllib.request.urlopen(u, timeout=5)
 print("HTTP_STATUS", r.status)
except urllib.error.HTTPError as e:
 print("HTTP_STATUS", e.code)
'
    return $?
  fi
  if command -v python >/dev/null 2>&1; then
    python -c 'import urllib2
try:
 r=urllib2.urlopen(%s, timeout=5)
 print("HTTP_STATUS", r.getcode())
except urllib2.HTTPError as e:
 print("HTTP_STATUS", e.code)
'
    return $?
  fi
  if command -v busybox >/dev/null 2>&1; then
    busybox wget -S -O /dev/null -T 2 "$URL"
    return $?
  fi
  if [ -x "$TMPBIN" ]; then
    "$TMPBIN" -S -O /dev/null -T 2 --tries=1 "$URL"
    return $?
  fi
  return 127
}

if have_client; then
  run_probe
  exit $?
fi

# No HTTP client in this image. Install wget only if we are root and a package
# manager is already present. Never apk/apt upgrade. Never touch app files.
if [ "$(id -u 2>/dev/null || echo 1)" != "0" ]; then
  echo PROBE_ERROR no http client and not root
  exit 2
fi

installed=
if command -v apk >/dev/null 2>&1; then
  if apk add --no-cache --quiet wget >/dev/null 2>&1; then
    echo apk-wget > "$MARKER"
    installed=apk-wget
  fi
elif command -v apt-get >/dev/null 2>&1; then
  if DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends wget >/dev/null 2>&1; then
    echo apt-wget > "$MARKER"
    installed=apt-wget
  fi
elif command -v microdnf >/dev/null 2>&1; then
  if microdnf install -y wget >/dev/null 2>&1; then
    echo microdnf-wget > "$MARKER"
    installed=microdnf-wget
  fi
elif command -v yum >/dev/null 2>&1; then
  if yum install -y wget >/dev/null 2>&1; then
    echo yum-wget > "$MARKER"
    installed=yum-wget
  fi
fi

if [ -z "$installed" ]; then
  echo PROBE_ERROR no http client and install skipped
  exit 2
fi

echo PROBE_INSTALLED "$installed"
run_probe
status=$?
exit $status
`
