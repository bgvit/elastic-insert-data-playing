package main

import (
	"context"
	"log"
	"math/rand"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/go-faker/faker/v4"
	"github.com/joho/godotenv"
)

const (
	elasticIndexName = "employees"
)

type Employee struct {
	Id      int    `json:"id"`
	Name    string `json:"name"`
	Address string `json:"address"`
	Salary  int    `json:"salary"`
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading env file: %s", err)
	}

	var myEnvs map[string]string
	myEnvs, err = godotenv.Read()
	if err != nil {
		log.Fatalf("Error reading map of env: %s", err)
	}

	var ES_API_KEY = myEnvs["ES_API_KEY"]
	var esURL = myEnvs["ES_URL"]

	cfg := elasticsearch.Config{
		Addresses: []string{
			esURL,
		},
		APIKey: ES_API_KEY,
	}

	es, err := elasticsearch.NewTypedClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	es.Indices.Create("employee").Do(context.TODO())

	n := 1

	for n <= 1000 {

		employee := Employee{
			Id:      n,
			Name:    faker.Name(),
			Address: faker.GetRealAddress().Address,
			Salary:  rand.Int(),
		}

		_, addErr := es.Index(elasticIndexName).
			Request(employee).
			Do(context.TODO())

		if addErr != nil {
			log.Fatalf("Error adding document: %s", addErr)
		}

		n++

	}
}
