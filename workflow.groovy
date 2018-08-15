#!groovy
import groovy.xml.MarkupBuilder 

def xml = new MarkupBuilder()
def wfName = 'BI Data Ingestion Using Parquet Input'
def wfXmlns = 'uri:oozie:workflow:0.5'
def actionXmlns = 'uri:oozie:action:0.1'
def libDir = '/user/bi-dev-ci/businessIndex/lib'

// Compose the builder
xml.'workflow-app'(name: wfName, xmlns: wfXmlns) {
    start(to: 'preprocess_links')
    kill(name: 'Kill') {
        message('Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]')
    }
    action(name: 'preprocess_links') {
        shell(xmlns: actionXmlns) {
            'job-tracker'('${jobTracker}')
            'name-node'('${nameNode}')
            'exec'("${libDir}/preprocess_links_app_parguet.sh")
            'file'("${libDir}/preprocess_links_app_parguet.sh#preprocess_links_app_parguet.sh")
            'file'("${libDir}/business-index-dataload_2.11-1.6.jar#business-index-dataload_2.11-1.6.jar")
            'file'("${libDir}/config-1.3.2.jar#config-1.3.2.jar")
            'capture-output'()
        }
        'ok'(to: 'link_data')
        'error'(to: 'Kill')
    }
    action(name: 'link_data') {
        shell(xmlns: actionXmlns) {
            'job-tracker'('${jobTracker}')
            'name-node'('${nameNode}')
            'exec'("${libDir}/link_data_app_v2.sh")
            'file'("${libDir}/link_data_app_v2.sh#link_data_app_v2.sh")
            'file'("${libDir}/business-index-dataload_2.11-1.6.jar#business-index-dataload_2.11-1.6.jar")
            'capture-output'()
        }
        'ok'(to: 'fork-ca1c')
        'error'(to: 'Kill')
    }
    action(name: 'load_bi_to_es') {
        shell(xmlns: actionXmlns) {
            'job-tracker'('${jobTracker}')
            'name-node'('${nameNode}')
            'exec'("${libDir}/load_bi_to_es_app_v2.sh")
            'file'("${libDir}/load_bi_to_es_app_v2.sh#load_bi_to_es_app_v2.sh")
            'file'("${libDir}/business-index-dataload_2.11-1.6.jar#business-index-dataload_2.11-1.6.jar")
            'file'("${libDir}/elasticsearch-spark_2.10-2.4.4.jar#elasticsearch-spark_2.10-2.4.4.jar")
            'capture-output'()
        }
        'ok'(to: 'join-0031')
        'error'(to: 'Kill')
    }
    action(name: 'hmrc_csv') {
        shell(xmlns: actionXmlns) {
            'job-tracker'('${jobTracker}')
            'name-node'('${nameNode}')
            'exec'("${libDir}/hmrc_csv_app_v2.sh")
            'file'("${libDir}/hmrc_csv_app_v2.sh#hmrc_csv_app_v2.sh")
            'file'("${libDir}/business-index-dataload_2.11-1.6.jar#business-index-dataload_2.11-1.6.jar")
            'file'("${libDir}/config-1.3.2.jar#config-1.3.2.jar")
            'capture-output'()
        }
        'ok'(to: 'join-0031')
        'error'(to: 'Kill')
    }
    fork(name: 'fork-ca1c') {
        path(start: 'load_bi_to_es')
        path(start: 'hmrc_csv')
    }
    join(name: 'join-0031', to: 'End')
    end(name: 'End')
}
