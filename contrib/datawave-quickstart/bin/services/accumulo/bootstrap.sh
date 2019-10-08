# Sourced by env.sh

#
# You should consider modifying these OS settings:
#
#   - Set system swappiness to 10 or below. Default is usually 60
#       which Accumulo will definitely complain about.
#
#   - Set max open files ('ulimit -n') to around 33K. Default is
#       1K which is too low in most cases
#

DW_ACCUMULO_SERVICE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Zookeeper config

# You may override DW_ZOOKEEPER_DIST_URI in your env ahead of time, and set as file:///path/to/file.tar.gz for local tarball, if needed
DW_ZOOKEEPER_DIST_URI="${DW_ZOOKEEPER_DIST_URI:-http://archive.cloudera.com/cdh5/cdh/5/zookeeper-3.4.5-cdh5.9.1.tar.gz}"
DW_ZOOKEEPER_DIST="$( downloadTarball "${DW_ZOOKEEPER_DIST_URI}" "${DW_ACCUMULO_SERVICE_DIR}" && echo "${tarball}" )"
DW_ZOOKEEPER_BASEDIR="zookeeper-install"
DW_ZOOKEEPER_SYMLINK="zookeeper"

# zoo.cfg...
DW_ZOOKEEPER_CONF="
tickTime=2000
syncLimit=5
clientPort=2181
dataDir=${DW_CLOUD_DATA}/zookeeper
maxClientCnxns=100"

# Accumulo config

# You may override DW_ACCUMULO_DIST_URI in your env ahead of time, and set as file:///path/to/file.tar.gz for local tarball, if needed
DW_ACCUMULO_DIST_URI=https://www.apache.org/dist/accumulo/2.0.0/accumulo-2.0.0-bin.tar.gz

DW_ACCUMULO_DIST="$( downloadTarball "${DW_ACCUMULO_DIST_URI}" "${DW_ACCUMULO_SERVICE_DIR}" && echo "${tarball}" )"
DW_ACCUMULO_BASEDIR="accumulo-install"
DW_ACCUMULO_SYMLINK="accumulo"
DW_ACCUMULO_INSTANCE_NAME="my-instance-01"
DW_ACCUMULO_PASSWORD="${DW_ACCUMULO_PASSWORD:-secret}"

alias ashell="accumulo shell -u root -p ${DW_ACCUMULO_PASSWORD}"

# Note that example configuration is provided for setting up VFS classpath for DataWave jars,
# but it is disabled by default, as it doesn't really buy you anything on a standalone cluster.
# To enable, set DW_ACCUMULO_VFS_DATAWAVE_ENABLED to true. If enabled, just be aware that
# writing all the DataWave jars to HDFS will probably slow down your install significantly

DW_ACCUMULO_VFS_DATAWAVE_ENABLED=false
DW_ACCUMULO_VFS_DATAWAVE_DIR="/datawave/accumulo-vfs-classpath"

# accumulo-site.xml (Format: <property-name><space><property-value>{<newline>})

DW_ACCUMULO_SITE_CONF="instance.volumes ${DW_HADOOP_DFS_URI}/accumulo
instance.zookeeper.host localhost:2181
instance.secret ${DW_ACCUMULO_PASSWORD}
tserver.memory.maps.max 385M
tserver.memory.maps.native.enabled false
tserver.cache.data.size 64M
tserver.cache.index.size 64M
trace.token.property.password ${DW_ACCUMULO_PASSWORD}
trace.user root"

if [ "${DW_ACCUMULO_VFS_DATAWAVE_ENABLED}" != false ] ; then
  DW_ACCUMULO_SITE_CONF="${DW_ACCUMULO_SITE_CONF}
general.vfs.context.classpath.datawave ${DW_HADOOP_DFS_URI}${DW_ACCUMULO_VFS_DATAWAVE_DIR}/.*.jar"
fi

DW_ACCUMULO_CLIENT_CONF="instance.zookeepers=localhost:2181
instance.name=${DW_ACCUMULO_INSTANCE_NAME}
auth.type=${DW_ACCUMULO_PASSWORD}
auth.principal=root
auth.token=secret"

DW_ACCUMULO_JVM_HEAPDUMP_DIR="${DW_CLOUD_DATA}/heapdumps"

DW_ACCUMULO_TSERVER_OPTS="\${POLICY} -Xmx768m -Xms768m -XX:-HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${DW_ACCUMULO_JVM_HEAPDUMP_DIR} -XX:+UseCompressedOops "

export ZOOKEEPER_HOME="${DW_CLOUD_HOME}/${DW_ZOOKEEPER_SYMLINK}"
export ACCUMULO_HOME="${DW_CLOUD_HOME}/${DW_ACCUMULO_SYMLINK}"
export PATH=${ACCUMULO_HOME}/bin:${ZOOKEEPER_HOME}/bin:$PATH

# Service helper variables and functions....

DW_ZOOKEEPER_CMD_START="( cd ${ZOOKEEPER_HOME}/bin && ./zkServer.sh start )"
DW_ZOOKEEPER_CMD_STOP="( cd ${ZOOKEEPER_HOME}/bin && ./zkServer.sh stop )"
DW_ZOOKEEPER_CMD_FIND_ALL_PIDS="pgrep -f 'zookeeper.server.quorum.QuorumPeerMain'"

DW_ACCUMULO_CMD_START="( cd ${ACCUMULO_HOME}/bin && ./accumulo-cluster start )"
DW_ACCUMULO_CMD_STOP="( cd ${ACCUMULO_HOME}/bin && ./accumulo-cluster stop )"
DW_ACCUMULO_CMD_FIND_ALL_PIDS="pgrep -f 'o.start.Main master|o.start.Main tserver|o.start.Main monitor|o.start.Main gc|o.start.Main tracer'"

function accumuloIsRunning() {
    DW_ACCUMULO_PID_LIST="$(eval "${DW_ACCUMULO_CMD_FIND_ALL_PIDS} -d ' '")"

    zookeeperIsRunning

    [[ -z "${DW_ACCUMULO_PID_LIST}" && -z "${DW_ZOOKEEPER_PID_LIST}" ]] && return 1 || return 0
}

function accumuloStart() {
    accumuloIsRunning && echo "Accumulo is already running" && return 1

    if ! zookeeperIsRunning ; then
       zookeeperStart
       echo
    fi
    if ! hadoopIsRunning ; then
       hadoopStart
       echo
    fi
    eval "${DW_ACCUMULO_CMD_START}"
    echo
    info "For detailed status visit 'http://localhost:9995' in your browser"
}

function accumuloStop() {
    accumuloIsRunning && [ ! -z "${DW_ACCUMULO_PID_LIST}" ] && eval "${DW_ACCUMULO_CMD_STOP}" || echo "Accumulo is already stopped"
    zookeeperStop
}

function accumuloStatus() {
    # define vars for accumulo processes
    local _gc
    local _master
    local _monitor
    local _tracer
    local _tserver

    echo "======  Accumulo Status  ======"
    local _opt=pid
    local _arg

    accumuloIsRunning
    test -n "${DW_ACCUMULO_PID_LIST}" && {
        local -r _pids=${DW_ACCUMULO_PID_LIST// /|}
        echo "pids: ${DW_ACCUMULO_PID_LIST}"

        for _arg in $(jps -lm | egrep "${_pids}"); do
            case ${_opt} in
                pid)
                    _pid=${_arg}
                    _opt=class;;
                class) _opt=component;;
                component)
                    local _none
                    case "${_arg}" in
                        gc) _gc=${_pid};;
                        master) _master=${_pid};;
                        monitor) _monitor=${_pid};;
                        tracer) _tracer=${_pid};;
                        tserver) _tserver=${_pid};;
                        *) _none=true;;
                    esac

                    test -z "${_none}" && info "${_arg} => ${_pid}"
                    _opt=address
                    unset _none
                    _pid=;;
                address) _opt=addrValue;;
                addrValue) _opt=pid;;
            esac
        done
    }

    test -z "${_gc}" && error "accumulo gc is not running"
    test -z "${_master}" && error "accumulo master is not running"
    test -z "${_monitor}" && info "accumulo monitor is not running"
    test -z "${_tracer}" && info "accumulo tracer is not running"
    test -z "${_tserver}" && error "accumulo tserver is not running"

    echo "======  Zookeeper Status  ======"
    if [[ -n "${DW_ZOOKEEPER_PID_LIST}" ]]; then
        info "zookeeper => ${DW_ZOOKEEPER_PID_LIST}"
    else
        error "zookeeper is not running"
    fi
}

function accumuloUninstall() {
    # Remove accumulo
    if accumuloIsInstalled ; then
       if [ -L "${DW_CLOUD_HOME}/${DW_ACCUMULO_SYMLINK}" ] ; then
           ( cd "${DW_CLOUD_HOME}" && unlink "${DW_ACCUMULO_SYMLINK}" ) || error "Failed to remove Accumulo symlink"
       fi

       if [ -d "${DW_ACCUMULO_SERVICE_DIR}/${DW_ACCUMULO_BASEDIR}" ] ; then
           rm -rf "${DW_ACCUMULO_SERVICE_DIR}/${DW_ACCUMULO_BASEDIR}"
       fi

       ! accumuloIsInstalled && info "Accumulo uninstalled" || error "Failed to uninstall Accumulo"
    else
      info "Accumulo not installed. Nothing to do"
    fi

    # Remove zookeeper
    if zookeeperIsInstalled ; then
       if [ -L "${DW_CLOUD_HOME}/${DW_ZOOKEEPER_SYMLINK}" ] ; then
           ( cd "${DW_CLOUD_HOME}" && unlink "${DW_ZOOKEEPER_SYMLINK}" ) || error "Failed to remove ZooKeeper symlink"
       fi

       if [ -d "${DW_ACCUMULO_SERVICE_DIR}/${DW_ZOOKEEPER_BASEDIR}" ] ; then
           rm -rf "${DW_ACCUMULO_SERVICE_DIR}/${DW_ZOOKEEPER_BASEDIR}"
       fi

       ! zookeeperIsInstalled && info "ZooKeeper uninstalled" || error "Failed to uninstall ZooKeeper"
    else
       info "ZooKeeper not installed. Nothing to do"
    fi

    [[ "${1}" == "${DW_UNINSTALL_RM_BINARIES_FLAG_LONG}" || "${1}" == "${DW_UNINSTALL_RM_BINARIES_FLAG_SHORT}" ]] && rm -f "${DW_ACCUMULO_SERVICE_DIR}"/*.tar.gz
}

function accumuloInstall() {
    ${DW_ACCUMULO_SERVICE_DIR}/install.sh
}

function zookeeperIsInstalled() {
    [ -L "${DW_CLOUD_HOME}/${DW_ZOOKEEPER_SYMLINK}" ] && return 0
    [ -d "${DW_ACCUMULO_SERVICE_DIR}/${DW_ZOOKEEPER_BASEDIR}" ] && return 0
    return 1
}

function accumuloIsInstalled() {
    [ -L "${DW_CLOUD_HOME}/${DW_ACCUMULO_SYMLINK}" ] && return 0
    [ -d "${DW_ACCUMULO_SERVICE_DIR}/${DW_ACCUMULO_BASEDIR}" ] && return 0
    return 1
}

function zookeeperIsRunning() {
    DW_ZOOKEEPER_PID_LIST="$(eval "${DW_ZOOKEEPER_CMD_FIND_ALL_PIDS} -d ' '")"
    [ -z "${DW_ZOOKEEPER_PID_LIST}" ] && return 1 || return 0
}

function zookeeperStart() {
    zookeeperIsRunning && echo "ZooKeeper is already running" || eval "${DW_ZOOKEEPER_CMD_START}"
}

function zookeeperStop() {
    zookeeperIsRunning && eval "${DW_ZOOKEEPER_CMD_STOP}" || echo "ZooKeeper is already stopped"
}

function zookeeperStatus() {
    zookeeperIsRunning && echo "ZooKeeper is running. PIDs: ${DW_ZOOKEEPER_PID_LIST}" || echo "ZooKeeper is not running"
}

function accumuloPrintenv() {
   echo
   echo "Accumulo Environment"
   echo
   ( set -o posix ; set ) | grep -E "ACCUMULO_|ZOOKEEPER_"
   echo
}

function accumuloPidList() {
   # Refresh pid lists
   accumuloIsRunning
   zookeeperIsRunning
   if [[ -n "${DW_ACCUMULO_PID_LIST}" || -n "${DW_ZOOKEEPER_PID_LIST}" ]] ; then
      echo "${DW_ACCUMULO_PID_LIST} ${DW_ZOOKEEPER_PID_LIST}"
   fi
}
