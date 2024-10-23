package rabbitmq

type RabbitMQConfig struct {
	URI         string
	Exchange    string
	RoutingKey  string
	Queue       string
	DLXExchange string
	DLXKey      string
}

type Task struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	URL      string `json:"url"`
}
