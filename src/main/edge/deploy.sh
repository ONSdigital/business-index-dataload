#!/usr/bin/env bash
 
# Exit immediately if a pipeline returns non-zero
set -o errexit

# Print a helpful message if a pipeline with non-zero exit code causes the
# script to exit as described above.
trap 'echo "Aborting due to errexit on line $LINENO. Exit code: $?" >&2' ERR

# Allow the above trap be inherited by all functions in the script.
set -o errtrace 

# Return value of a pipeline is the value of the last (rightmost) command to
# exit with a non-zero status
set -o pipefail

arg_repo="${1}"
arg_archive_base_name="${2}"
arg_version="${3}"
arg_artifactory_url=${4}
arg_artifactory_usr="${5}"
arg_artifactory_pwd="${6}"
arg_hdfs_usr="${7}"
arg_repo_branch="${8}"

__required_num_args=6
__local_user_home="/home/${arg_hdfs_usr}@Ons.Statistics.gov.uk"
__local_download_base="${__local_user_home}/download/oozie"
__local_install_base="${__local_user_home}/install/oozie"
__local_install_version="${__local_install_base}/${arg_archive_base_name}-${arg_version}-${arg_repo_branch}/"
__hdfs_install_base="/user/${arg_hdfs_usr}/applications/oozie"
__group_path="uk/gov/ons"
__repository_path="${arg_repo}/${__group_path}/${arg_archive_base_name}/${arg_version}"

###############################################################################
# Program Functions
###############################################################################

_print_help() {
  cat <<HEREDOC
Deploy Oozie workflows to HDFS
Usage:
  ${__me} <repo> <archive_base_name> <version> <art_url> <art_usr> <art_pwd> <hdfs_usr>
HEREDOC
}

_banner() {
    cat <<BANNER
      _            _                   _     
     | |          | |                 | |    
   __| | ___ _ __ | | ___  _   _   ___| |__  
  / _  |/ _ \ '_ \| |/ _ \| | | | / __| '_ \ 
 | (_| |  __/ |_) | | (_) | |_| |_\__ \ | | |
  \__,_|\___| .__/|_|\___/ \__, (_)___/_| |_|
            | |             __/ |            
            |_|            |___/             
BANNER
}

_create_base_dirs() {
    printf "Creating base directories.\\n"
    mkdir -p "${__local_download_base}/"
    mkdir -p "${__local_install_base}/"
    hdfs dfs -mkdir -p "${__hdfs_install_base}/${arg_archive_base_name}-${arg_version}/"
    hdfs dfs -mkdir -p "${__hdfs_install_base}/"
    hdfs dfs -chown -R "${arg_hdfs_usr}:${arg_hdfs_usr}" "${__hdfs_install_base}/${arg_archive_base_name}-${arg_version}/"
}

_download_latest() {
    printf "Downloading latest code.\\n"
    cd "${__local_download_base}" || return
    curl --fail -u"${arg_artifactory_usr}":"${arg_artifactory_pwd}" -O "${arg_artifactory_url}/${__repository_path}/${arg_archive_base_name}-${arg_version}-hdfs.zip"
}

_extract_archive() {
    printf "Extracting archives.\\n"
    unzip "${arg_archive_base_name}-${arg_version}-hdfs.zip" -d "${__local_install_version}"
}

_deploy_latest() {
    printf "Deploying latest.\\n"
    hdfs dfs -mv "${__hdfs_install_base}/${arg_archive_base_name}-${arg_version}" "${__hdfs_install_base}/${arg_archive_base_name}-latest"
}

_deploy_hdfs() {
    printf "Move latest release to HDFS.\\n"
    hdfs dfs -copyFromLocal "${__local_install_version}/apps" "${__hdfs_install_base}/${arg_archive_base_name}-${arg_version}/"
    
    # Backup current latest if one exists and deploy, else just deploy
    if hadoop fs -test -d "${__hdfs_install_base}/${arg_archive_base_name}-latest/"; then
        printf "Backing up current deployment.\\n"
        hdfs dfs -mv "${__hdfs_install_base}/${arg_archive_base_name}-latest/" "${__hdfs_install_base}/${arg_archive_base_name}-$(date +%Y-%M-%d-%H-%m-%S)"
        _deploy_latest
    else
        _deploy_latest
    fi
}

###############################################################################
# Main
###############################################################################

# Print help message if we recieve an incorrect number of arguments
if [ $# -ne ${__required_num_args} ] ; then
    _print_help
fi

_main() {
    _banner
    _create_base_dirs "$@"
    _download_latest "$@"
    _extract_archive "$@"
    _deploy_hdfs "$@"
}

# Call `_main` after everything has been defined.
_main "$@"