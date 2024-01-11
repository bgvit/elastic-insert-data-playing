package main

import (
	"context"
	"log"
	"math/rand"
	"strconv"

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

	myEnvs := getEnvs()
	es := createESClient(myEnvs)
	validateExistenceOfIndiceOrCreateIt(es)
	populateESDatabase(es, myEnvs)
}

func getEnvs() map[string]string {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading env file: %s", err)
	}

	var myEnvs map[string]string
	myEnvs, err = godotenv.Read()
	if err != nil {
		log.Fatalf("Error reading map of env: %s", err)
	}
	return myEnvs
}

func createESClient(myEnvs map[string]string) *elasticsearch.TypedClient {

	var ES_API_KEY = myEnvs["ES_API_KEY"]
	var esURL = myEnvs["ES_URL"]

	cfg := elasticsearch.Config{
		Addresses: []string{
			esURL,
		},
		APIKey: ES_API_KEY,
	}

	esClient, err := elasticsearch.NewTypedClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	return esClient
}

func validateExistenceOfIndiceOrCreateIt(es *elasticsearch.TypedClient) {
	exists, err := es.Indices.Exists(elasticIndexName).Do(context.TODO())
	if err != nil {
		log.Fatalf("Error trying to discover if indice exists: %s", err)
	}

	if !exists {
		es.Indices.Create(elasticIndexName).Do(context.TODO())
	}
}

func createFakeEmployee(n int) Employee {
	return Employee{
		Id:      n,
		Name:    faker.Name(),
		Address: faker.GetRealAddress().Address,
		Salary:  rand.Int(),
	}
}

func addEmployee(es *elasticsearch.TypedClient, employee *Employee) {
	_, err := es.Index(elasticIndexName).
		Request(employee).
		Do(context.TODO())

	if err != nil {
		log.Fatalf("Error adding document: %s", err)
	}
}

func populateESDatabase(es *elasticsearch.TypedClient, myEnvs map[string]string) {

	n := 1
	for n <= getTotalEmployeeNumberToPopulate(myEnvs) {
		employee := createFakeEmployee(n)
		addEmployee(es, &employee)
		n++
	}
}

func getTotalEmployeeNumberToPopulate(myEnvs map[string]string) int {
	var numberToPopulateString = myEnvs["POPULATE_NUMBER_EMPLOYEES"]
	populateEmployeesNumber, err := strconv.Atoi(numberToPopulateString)
	if err != nil {
		log.Fatalf("Error converting env string to int: %s", err)
	}
	return populateEmployeesNumber
}
