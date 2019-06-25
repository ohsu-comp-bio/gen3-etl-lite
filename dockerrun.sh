
sleep 2
until (curl -k $ELASTICSEARCH_URL) >/dev/null 2>&1; do
  echo "esproxy-service is unavailable - sleeping "
  sleep 2
done
echo "esproxy-service is available"


# writes default template - all strings are considered keywords
curl -k -X PUT -H "Content-Type: application/json"  $ELASTICSEARCH_URL/_template/template_1 -d '
{
  "index_patterns" : ["biospecimen", "data_file", "survey", "case"],
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
        },
        {
          "sample_Box_as_string": {
            "path_match":   "sample.Box",
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

echo DICTIONARY_URL: $DICTIONARY_URL
echo PATH_TO_SCHEMA_DIR: $PATH_TO_SCHEMA_DIR

cd gen3_replicator

python gen3_inventory.py
