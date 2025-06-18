package pulsar

import "encoding/json"

func MapConfigToStruct(config map[string]interface{}, conf interface{}) error {
	bytes, err := json.Marshal(config)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, conf)
}
