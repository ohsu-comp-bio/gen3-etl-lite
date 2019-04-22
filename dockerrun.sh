
sleep 2
until (curl http://esproxy-service:9200) >/dev/null 2>&1; do
  echo "esproxy-service is unavailable - sleeping"
  sleep 2
done
echo "esproxy-service is available"


# writes default template - all strings are considered keywords
curl -X PUT -H "Content-Type: application/json"  http://esproxy-service:9200/_template/template_1 -d '
{
  "index_patterns" : ["biospecimen", "data_file"],
  "order" : 0,
  "mappings": {
    "_default_": {
      "dynamic_templates": [
        {
          "strings_as_keywords": {
            "match_mapping_type": "string",
            "mapping": {
              "type": "keyword"
            }
          }
        }
      ]
    }
  }
}' >/dev/null 2>&1

echo "wrote default index to esproxy-service"
echo

cd gen3_replicator

python gen3_replicator.py
