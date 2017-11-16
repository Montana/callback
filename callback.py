import collectd
import requests
import json
import numbers

DB_METRICS = ('data_size', 'doc_count', 'doc_del_count', 'disk_size')

def _type(key, subkey):
    if key == 'httpd_request_methods':
        return "http_request_methods"
    elif key == 'httpd_status_codes':
        return "http_response_codes"
    elif subkey.endswith("requests"):
        return"http_requests"
    else:
        return "gauge"


def configure_callback(configuration, conf):
    collectd.debug("CouchDB plugin configure callback")
    for node in conf.children:
        if node.key.lower() == 'url':
            configuration['url'] = node.values[0].rstrip("/")
        else:
            raise RuntimeError("Unknown configuration key %s" % node.key)

def read_callback(configuration):
    r = requests.get(configuration['url'] + "/_stats")
    for key, data in r.json().iteritems():
        for subkey, metrics in data.iteritems():
            for m_type, value in metrics.iteritems():
                if isinstance(value, numbers.Number):
                    val = collectd.Values(plugin='couchdb', type=_type(key, subkey))
                    val.plugin_instance = key + "_" + subkey
                    val.type_instance = m_type
                    val.values = [value]
                    val.dispatch()
    dbs = set(requests.get(configuration['url'] + "/_all_dbs").json())
    for db in dbs ^ set(['_replicator', '_users']):
        metrics = requests.get(configuration['url'] + "/" + db).json()
        for metric in DB_METRICS:
            if metric in metrics:
                val = collectd.Values(plugin='couchdb', type="gauge", plugin_instance='db_stats')
                val.values = [metrics[metric]]
                val.type_instance = metric
                val.dispatch()

configuration = {}
collectd.register_config(lambda conf: configure_callback(configuration, conf))
collectd.register_read(read_callback, 10, configuration)
