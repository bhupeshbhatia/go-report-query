package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/TerrexTech/go-commonutils/commonutil"

	"github.com/bhupeshbhatia/go-agg-inventory-v2/model"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

const AGGREGATE_ID = 2

type Env struct {
	db model.Datastore
}

func ErrorStackTrace(err error) string {
	return fmt.Sprintf("%+v\n", err)
}

// func initRoutes() *mux.Router {
// 	router := mux.NewRouter()
// 	router = setAuthenticationRoute(router)
// 	return router
// }

// func setAuthenticationRoute(router *mux.Router, env *Env) *mux.Router {
// 	// router.HandleFunc("/add-product", service.AddInventory).Methods("POST", "OPTIONS")
// 	// router.HandleFunc("/update-product", service.UpdateInventory).Methods("POST", "OPTIONS")
// 	// router.HandleFunc("/delete-product", service.DeleteInventory).Methods("POST", "OPTIONS")
// 	// router.HandleFunc("/search-range", service.TimeSearchInTable).Methods("POST", "OPTIONS")
// 	router.HandleFunc("/create-data", env.LoadDataInMongo).Methods("GET", "OPTIONS")
// 	router.HandleFunc("/load-table", env.LoadInventoryTable).Methods("POST", "OPTIONS")
// 	// router.HandleFunc("/dist-weight", service.DistributionByWeight).Methods("GET", "OPTIONS")
// 	// router.HandleFunc("/twsalewaste", service.TotalWeightSoldWasteDonatePerDay).Methods("POST", "OPTIONS")
// 	// router.HandleFunc("/search-table", service.SearchInvTable).Methods("POST", "OPTIONS")

// 	// router.HandleFunc("/perhr-sale", service.ProdSoldPerHour).Methods("POST", "OPTIONS")

// 	return router
// }

func main() {
	err := godotenv.Load()
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"MONGO_HOSTS",
		"MONGO_DATABASE",
		"MONGO_COLLECTION",
		// "MONGO_TIMEOUT",
	)
	if err != nil {
		log.Fatalf(
			"Error: Environment variable %s is required but was not found", missingVar,
		)
	}

	hosts := os.Getenv("MONGO_HOSTS")
	username := os.Getenv("MONGO_USERNAME")
	password := os.Getenv("MONGO_PASSWORD")
	database := os.Getenv("MONGO_DATABASE")
	collection := os.Getenv("MONGO_COLLECTION")

	config := model.DbConfig{
		Hosts:      *commonutil.ParseHosts(hosts),
		Username:   username,
		Password:   password,
		Database:   database,
		Collection: collection,
	}

	//Db IO
	db, err := model.ConfirmDbExists(config)
	if err != nil {
		err = errors.Wrap(err, "Error connecting to Inventory DB")
		log.Println(err)
		return
	}

	//This Env is in file route_handlers.go
	env := &Env{db}

	// router := mux.NewRouter()
	// router = setAuthenticationRoute(router, env)

	// n := negroni.Classic()
	// n.UseHandler(router)

	// http.ListenAndServe(":8080", n)

	http.HandleFunc("/create-data", env.LoadDataInMongo)
	http.HandleFunc("/load-table", env.LoadInventoryTable)
	http.HandleFunc("/add-inv", env.AddInv)
	http.HandleFunc("/up-inv", env.UpdateInv)
	http.HandleFunc("/del-inv", env.DeleteInv)
	http.HandleFunc("/total-inv", env.TotalGraph)
	http.HandleFunc("/sold-inv", env.SoldPerHr)
	http.HandleFunc("/dist-inv", env.DistWeight)
	http.HandleFunc("/search-inv", env.SearchTable)
	http.HandleFunc("/gen-data", env.GenDataForAdd)
	http.ListenAndServe(":8080", nil)
}

func (env *Env) LoadDataInMongo(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	}
	// Stop here if its Preflighted OPTIONS request
	if r.Method == "OPTIONS" {
		return
	}

	// DB connection
	insertedData, err := env.db.CreateDataMongo(100)
	if err != nil {
		err = errors.Wrap(err, "Unable to create new data in mongo")
		log.Println(err)
		return
	}
	w.Write(insertedData)
}

func (env *Env) GenDataForAdd(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	}
	// Stop here if its Preflighted OPTIONS request
	if r.Method == "OPTIONS" {
		return
	}

	totalResult, err := env.db.GenForAddInv()
	if err != nil {
		err = errors.Wrap(err, "Unable to read the request body")
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(totalResult)
}

func (env *Env) LoadInventoryTable(w http.ResponseWriter, r *http.Request) {

	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	}
	// Stop here if its Preflighted OPTIONS request
	if r.Method == "OPTIONS" {
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		err = errors.Wrap(err, "Unable to read the request body")
		log.Println(err)
		return
	}

	totalResult, err := env.db.SearchByDate(body)
	if err != nil {
		err = errors.Wrap(err, "Unable to get results - LoadInvTable")
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(totalResult)
}

func (env *Env) SearchTable(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	}
	// Stop here if its Preflighted OPTIONS request
	if r.Method == "OPTIONS" {
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		err = errors.Wrap(err, "Unable to read the request body")
		log.Println(err)
		return
	}

	invAfterSearch, err := env.db.SearchByKeyVal(body)
	if err != nil {
		err = errors.Wrap(err, "Unable to read the request body")
		log.Println(err)
		return
	}

	w.Write(invAfterSearch)
}

func (env *Env) AddInv(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	}
	// Stop here if its Preflighted OPTIONS request
	if r.Method == "OPTIONS" {
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		err = errors.Wrap(err, "Unable to read the request body")
		log.Println(err)
		return
	}

	insertResult, err := env.db.AddInventory(body)
	if err != nil {
		err = errors.Wrap(err, "Unable to insert in inventory")
		log.Println(err)
		w.Write(insertResult)
		return
	}

	w.Write(insertResult)
}

func (env *Env) UpdateInv(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	}
	// Stop here if its Preflighted OPTIONS request
	if r.Method == "OPTIONS" {
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		err = errors.Wrap(err, "Unable to read the request body")
		log.Println(err)
		return
	}

	updateResult, err := env.db.UpdateInventory(body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}

	result := strconv.Itoa(int(updateResult.ModifiedCount))

	w.Write([]byte(result))
}

func (env *Env) DeleteInv(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	}
	// Stop here if its Preflighted OPTIONS request
	if r.Method == "OPTIONS" {
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		err = errors.Wrap(err, "Unable to read the request body")
		log.Println(err)
		return
	}

	delResult, err := env.db.DeleteInventory(body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}

	w.Write(delResult)
}

func (env *Env) TotalGraph(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	}
	// Stop here if its Preflighted OPTIONS request
	if r.Method == "OPTIONS" {
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		err = errors.Wrap(err, "Unable to read the request body")
		log.Println(err)
		return
	}

	_, err = env.db.CreateDataMongo(1)
	if err != nil {
		err = errors.Wrap(err, "Unable to insert new data - TotalGraph")
		log.Println(err)
		return
	}

	invAfterSearch, err := env.db.SearchByDate(body)
	if err != nil {
		err = errors.Wrap(err, "Unable to read the request body")
		log.Println(err)
		return
	}

	results, err := env.db.CompareInvGraph(body, invAfterSearch)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.Write(results)
}

func (env *Env) SoldPerHr(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	}
	// Stop here if its Preflighted OPTIONS request
	if r.Method == "OPTIONS" {
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		err = errors.Wrap(err, "Unable to read the request body")
		log.Println(err)
		return
	}

	invAfterSearch, err := env.db.SearchByDate(body)
	if err != nil {
		err = errors.Wrap(err, "Unable to read the request body")
		log.Println(err)
		return
	}

	results, err := env.db.ProdSoldPerHour(body, invAfterSearch)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.Write(results)
}

func (env *Env) DistWeight(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	}
	// Stop here if its Preflighted OPTIONS request
	if r.Method == "OPTIONS" {
		return
	}

	// body, err := ioutil.ReadAll(r.Body)
	// if err != nil {
	// 	err = errors.Wrap(err, "Unable to read the request body")
	// 	log.Println(err)
	// 	return
	// }

	results, err := env.db.DistByWeight()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.Write(results)
}

//----------------------------------------------------------

//Calling Mongo
// Db, err := connectDB.ConfirmDbExists()
// if err != nil {
// 	err = errors.Wrap(err, "Mongo client unable to connect")
// 	log.Println(err)
// }

// mgCollection := Db.Collection

// mockData := mockdata.JsonForAddProduct()
// inventory, err := service.GetInventoryJSON([]byte(mockData))
// if err != nil {
// 	err = errors.Wrap(err, "Unable to unmarshal into Inventory struct")
// 	log.Println(err)
// }

// //Adding timestamp to inventory
// inventory.Timestamp = time.Now()

// insertData := &service.InventoryData{
// 	Product:     inventory,
// 	MongoTable:  mgCollection,
// 	FilterName:  "Fruit_ID",
// 	FilterValue: inventory.FruitID,
// }

// insertResult, err := service.AddProduct(*insertData)
// if err != nil {
// 	err = errors.Wrap(err, "Unable to insert event")
// 	log.Println(err)
// }

//Consumer
// config := consumer.Config{
// 	ConsumerGroup: "inventory.consumer.persistence",
// 	KafkaBrokers:  []string{"kafka:9092"},
// 	Topics:        []string{"event.rns_eventstore.events"},
// }

// kafka.KafkaConsumer(config)

// eventTopic := "events.rns_eventstore.events." + strconv.Itoa(AGGREGATE_ID)

// eventConfig := consumer.Config{
// 	ConsumerGroup: "events.rns_eventstore.eventsresponse",
// 	KafkaBrokers:  []string{"kafka:9092"},
// 	Topics:        []string{eventTopic},
// }

// kafka.KafkaConsumer(eventConfig)

// //Kafka Producer
// aggregateID := es.EventStoreQuery{
// 	AggregateID:      AGGREGATE_ID,
// 	AggregateVersion: 2,
// 	YearBucket:       2018,
// }

// producerJSON, err := json.Marshal(aggregateID)
// if err != nil {
// 	log.Println(err)
// }

// kafka.KafkaProducer(string(producerJSON))

// asyncProducer, err := kafka.ResponseProducer(kafka.KafAdapter{
// 	Address:         []string{"kafka:9092"},
// 	ProducerResChan: input,
// 	ResponseTopic:   "events.rns_eventstore.eventsquery",
// })
// if err != nil {
// 	err = errors.Wrap(err, "Unable to create producer")
// 	log.Println(ErrorStackTrace(err))
// }

// asyncProducer.EnableLogging()

// go func() {
// 	for err := asyncProducer.Errors() {
// 	  log.Println(err)
// 	}
//   }()

// aggregateID := es.EventStoreQuery{
// 	AggregateID:      AGGREGATE_ID,
// 	AggregateVersion: 2,
// 	YearBucket:       2018,
// }

// inputJson, err := json.Marshal(aggregateID)
// if err != nil {
// 	log.Println(err)
// }

// go func() {
// 	fmt.Println(asyncProducer)
// 	input <- &es.KafkaResponse{
// 		AggregateID: AGGREGATE_ID,
// 		Input:       string(inputJson),
// 	}
// }()
// time.Sleep(10 * time.Second)

//---------------------------------------------------------------------------------

// Creates a KafkaIO from KafkaAdapter based on set environment variables.

// input := make(chan *model.KafkaResponse)

// _, err := kafka.ResponseProducer(kafka.KafAdapter{
// 	Address:         []string{"kafka:9092"},
// 	ProducerResChan: input,
// 	ResponseTopic:   "test",
// })
// if err != nil {
// 	err = errors.Wrap(err, "Unable to create producer")
// 	log.Println(ErrorStackTrace(err))
// }

// go func() {
// 	// fmt.Println(produce)
// 	input <- &model.KafkaResponse{
// 		AggregateID: 1,
// 		Input:       "NOOOOOOOOOOO",
// 	}

// }()

//Consumer
// adap := kafka.KafkaConAdapter{
// 	Address:        []string{"kafka:9092"},
// 	ConsumerGroup:  "monitoring",
// 	ConsumerTopics: []string{"KafkaProducerTest"},
// }
// kio, err := kafka.Consume(&adap)
// go func() {
// 	for err := range kio.ConsumerErrChan {
// 		log.Println(err)
// 	}
// }()
// if err != nil {
// 	err = errors.Wrap(err, "Unable to create producer")
// 	log.Println(ErrorStackTrace(err))
// }

// var test []byte

// for msg := range kio.ConsumerMsgChan {
// 	log.Println(msg)
// 	test = msg.Value
// }

// fmt.Print(test)

// func CreateClientAndCollection() *mongo.Collection {
// 	client, err := connectDB.CreateClient()
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to get Mongo collection")
// 		log.Println(ErrorStackTrace(err))
// 	}

// 	mgTable, err := connectDB.CreateCollection(client, "users", "rns_aggregates")
// 	if err != nil {
// 		err = errors.Wrap(err, "Unable to insert in mongo")
// 		log.Println(ErrorStackTrace(err))
// 	}

// 	// aggVersion, err := events.GetMaxAggregateVersion(mgTable, aggregateID)
// 	// if err != nil {
// 	// 	err = errors.Wrap(err, "Mongo version not received")
// 	// 	log.Println(ErrorStackTrace(err))
// 	// }
// 	// return aggVersion

// 	return mgTable
// }

// mgTable := CreateClientAndCollection()
// inventory, err := service.GetInventoryJSON([]byte(mockdata.JsonForGetJSONString()))
// if err != nil {
// 	err = errors.Wrap(err, "Unable to unmarshal foodItem into Inventory struct")
// 	log.Println(err)
// }

// fmt.Println(inventory)

// _, err := service.GetInventoryJSON([]byte(mockdata.JsonForAddProduct()))
// if err != nil {
// 	err = errors.Wrap(err, "Unable to unmarshal addProduct json into Inventory struct")
// 	log.Println(err)
// }

// fmt.Printf("%+v", inventoryData)

// inv, err := service.GetMarshal(mockdata.InventoryMock())
// if err != nil {
// 	err = errors.Wrap(err, "Unable to unmarshal addProduct json into Inventory struct")
// 	log.Println(err)
// }

// testJson, err := service.GetInventoryJSON(inv)
// if err != nil {
// 	err = errors.Wrap(err, "Unable to unmarshal addProduct json into Inventory struct")
// 	log.Println(err)
// }

// fmt.Printf("%+v", testJson)

// timeWhenInserted := time.Now()

// inventoryInsert.Timestamp = timeWhenInserted

// insertData := &service.InventoryData{
// 	Product:     inventoryInsert,
// 	MongoTable:  mgTable,
// 	FilterName:  "Fruit_ID",
// 	FilterValue: inventoryInsert.FruitID,
// }

// insertResult, err := service.AddFood(*insertData)
// if err != nil {
// 	err = errors.Wrap(err, "Unable to unmarshal addProduct json into Inventory struct")
// 	log.Println(err)
// }
// fmt.Println("Insert: ", insertResult)

//=======================================================================================
//==========================================KAFKA==================================
