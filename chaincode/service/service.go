package main

import (
	"encoding/json"
	"fmt"
	"github.com/inklabsfoundation/inkchain/core/chaincode/shim"
	pb "github.com/inklabsfoundation/inkchain/protos/peer"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// Incentive-related const
const (
	IncentiveMashupInvoke = "10"
	FeeBalanceType        = "TOKENS"
	L                     = 2
	R                     = 1
)

// Definitions of a service's status
const (
	S_Created   = "created"
	S_Available = "available"
	S_Invalid   = "invalid"
)

// Prefixes for user and service separately
const (
	UserPrefix             = "USER_"
	ServicePrefix          = "SER_"
	ServiceCallTimesPrefix = "CALL_TIMES_"
	BuyRecordPrefix        = "BUY_"
	ReduceRecordPrefix     = "REDUCE_"
)

const (
	UserServicesKey = "userServicesKey" //composite key for user service composite
	CallTimeKey     = "callTimeKey"     //composite key for call time composite
)

// Invoke functions definition
const (
	// User-related basic invoke
	RegisterUser = "registerUser"
	RemoveUser   = "removeUser"
	QueryUser    = "queryUser"

	// Service-related invoke
	RegisterService     = "registerService"
	InvalidateService   = "invalidateService" // mark whether the service is validated
	PublishService      = "publishService"    // publish a created service
	CreateMashup        = "createMashup"      // utilize services to create a new mashup
	QueryService        = "queryService"
	EditService         = "editService"
	QueryServiceByUser  = "queryServiceByUser"
	QueryServiceByRange = "queryServiceByRange"
	CallService         = "callService"
	ReduceCallTime      = "reduceCallTime"
	GetCallTimes        = "getCallTimes"
	GetCallTime         = "getCallTime"

	// User-related reward invoke
	RewardService = "rewardService"
)

// Chaincode for DSES (Decentralized Service Eco-System)
type serviceChaincode struct {
}

// Structure definition for user
type user struct {
	Name         string `json:"name"`
	Introduction string `json:"introduction"`
	Address      string `json:"address"`
	// There is a one-to-one correspondence between "Name" and "Address"
	// The Address records the user's profit from creating valuable services or mashups.

	Contribution float64 `json:"contribution"`
	// "Contribution" evaluates the user's contribution to the service ecosystem.
	// TODO: add handler about "Contribution"
	// Benefit of "Contribution":
	// 1. construct a evaluation for every user's contribution on the service ecosystem
	// 2. inspire users to participate in creating new services and mashups
	TotalService     int `json:"totalService"`
	TotalCallTimes   int `json:"totalCallTimes"`
	TotalInvokeTimes int `json:"totalInvokeTimes"`
}

// Structure definition for service
// type "service" defines conventional services as well as mashups.
type service struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Developer   string   `json:"developer"` // record the user that developed this service
	Description string   `json:"description"`
	Resource    string   `json:"resource"` //service address
	Price       *big.Int `json:"price"`

	CreatedTime string `json:"createdTime"`
	UpdatedTime string `json:"updatedTime"`

	// Status records the status of a service:
	// created/available/invalid
	Status string `json:"status"`

	// Whether the service is a mashup or not.
	IsMashup bool `json:"isMashup"`

	// if the service is a mashup, "Composited" records the services that it invokes;
	// if the service is not a mashup, "Composited" records the co-occurrence documents of the service
	Composition map[string]int `json:"composition"`

	// Benefit of "Composited":
	// 1. Automatically create service co-occurrence documents and store it into the ledger
	// 2. Promote the security and integrality of service data

	// future: people need to pay if they want to use the record information
}

type serviceCallTime struct {
	ServiceName string   `json:"service_name"` // service name
	UserName    string   `json:"user_name"`    // user name
	UserAddress string   `json:"user_address"` // user address
	CallTimes   *big.Int `json:"call_times"`   // call times
	Total       *big.Int `json:"total"`        // total fee

	CreateTime string `json:"create_time"` //create time
	UpdateTime string `json:"update_time"` //last reduce time
}

type buyRecord struct {
	ServiceCallTimeKey string   `json:"service_call_time_key"`
	ServiceName        string   `json:"service_name"`
	UserName           string   `json:"user_name"`
	CallTime           *big.Int `json:"call_time"`
	Total              *big.Int `json:"total"`
	CreateTime         string   `json:"create_time"`
}

type reduceRecord struct {
	ServiceName        string   `json:"service_name"`
	ServiceCallTimeKey string   `json:"service_call_time_key"`
	UserName           string   `json:"user_name"`
	ReduceTime         *big.Int `json:"reduce_time"`
	CreateTime         string   `json:"create_time"`
}

// ===================================================================================
// Main
// ===================================================================================
func main() {
	err := shim.Start(new(serviceChaincode))
	if err != nil {
		fmt.Printf("Error starting assetChaincode: %s", err)
	}
}

// Init initializes chaincode
// ==================================================================================
func (t *serviceChaincode) Init(stub shim.ChaincodeStubInterface) pb.Response {
	fmt.Println("assetChaincode Init.")
	return shim.Success([]byte("Init success."))
}

// Invoke func
// ==================================================================================
func (t *serviceChaincode) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	fmt.Println("assetChaincode Invoke.")
	function, args := stub.GetFunctionAndParameters()

	switch function {
	// ********************************************************
	// PART 1: User-related invokes
	case RegisterUser:
		if len(args) != 2 {
			return shim.Error("Incorrect number of arguments. Expecting 2.")
		}
		// args[0]: user name
		return t.registerUser(stub, args)

	case RemoveUser:
		if len(args) != 1 {
			return shim.Error("Incorrect number of arguments. Expecting 1.")
		}
		// args[0]: user name
		return t.removeUser(stub, args)

	case QueryUser:
		if len(args) != 1 {
			return shim.Error("Incorrect number of arguments. Expecting 1.")
		}
		// args[0]: user name
		return t.queryUser(stub, args)

	// ********************************************************
	// PART 2: service-related invokes
	case RegisterService:
		if len(args) != 6 {
			return shim.Error("Incorrect number of arguments. Expecting 5.")
		}
		// args[0]: service name
		// args[1]: service type
		// args[2]: service description
		// args[3]: developer's name
		// args[4]: service path
		// args[5]: service price
		return t.registerService(stub, args)

	case InvalidateService:
		if len(args) != 1 {
			return shim.Error("Incorrect number of arguments. Expecting 1.")
		}
		// args[0]: service name
		return t.invalidateService(stub, args)

	case PublishService:
		if len(args) != 1 {
			return shim.Error("Incorrect number of arguments. Expecting 1.")
		}
		// args[0]: service name
		return t.publishService(stub, args)

	case QueryService:
		if len(args) != 1 {
			return shim.Error("Incorrect number of arguments. Expecting 1.")
		}
		// args[0]: service name
		return t.queryService(stub, args)

	case EditService:
		if len(args) != 5 {
			return shim.Error("Incorrect number of arguments. Expecting 5.")
		}
		// args[0]: service name
		// args[1]: service type
		// args[2]: service description
		// args[3]: service path
		// args[4]: service price
		return t.editService(stub, args)

	case CreateMashup:
		if len(args) < 5 {
			return shim.Error("Incorrect number of arguments. Expecting 5 at least.")
		}
		// args[0]: mashup name
		// args[1]: mashup type
		// args[2]: mashup description
		// args[3]: mashup price
		// args[4...]: invoked service list
		return t.createMashup(stub, args)

	case QueryServiceByRange:
		if len(args) != 2 {
			return shim.Error("Incorrect number of arguments. Expecting 2.")
		}
		// args[0]: begin index
		// args[1]: end index
		return t.queryServiceByRange(stub, args)

	// ********************************************************
	// PART 3: user-related reward invokes
	case RewardService:
		if len(args) < 3 {
			return shim.Error("Incorrect number of arguments. Expecting 3 at least.")
		}
		// args[0]: service name
		// args[1]: reward_type
		// args[2]: reward_amount
		return t.rewardService(stub, args)

	case QueryServiceByUser:
		if len(args) != 3 {
			return shim.Error("Incorrect number of arguments. Expecting 3.")
		}
		// args[0]: user_name
		return t.queryServiceByUser(stub, args)

	case CallService:
		if len(args) != 2 {
			return shim.Error("Incorrect number of arguments. Expecting 2.")
		}
		// args[0]: service name
		// args[1]: call times
		return t.callService(stub, args)

	case GetCallTime:
		if len(args) != 2 {
			return shim.Error("Incorrect number of arguments. Expecting 2.")
		}
		//args[0]: service name
		//args[1]: user name
		return t.getCallTime(stub, args)

	case GetCallTimes:
		if len(args) != 1 {
			return shim.Error("Incorrect number of arguments. Expecting 1.")
		}
		return t.getCallTimes(stub, args)

	case ReduceCallTime:
		if len(args) != 3 {
			return shim.Error("Incorrect number of arguments. Expecting 3.")
		}
		//args[0]: service name
		//args[1]: caller name
		//args[2]: reduce times
		return t.reduceCallTime(stub, args)
	}

	return shim.Error("Invalid invoke function.")
}

// Invoke func about user
// ==================================================================================

// ==================================
// registerUser: Register a new user
// ==================================
func (t *serviceChaincode) registerUser(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var new_name string
	var new_intro string
	var new_add string
	var err error

	new_name = args[0]
	new_intro = args[1]

	// Get the user's address automatically through INKchian's GetSender() interface
	new_add, err = stub.GetSender()
	if err != nil {
		return shim.Error("Fail to get the sender's address.")
	}

	// check if user exists
	user_key := UserPrefix + new_name
	userAsBytes, err := stub.GetState(user_key)
	if err != nil {
		return shim.Error("Fail to get user: " + err.Error())
	} else if userAsBytes != nil {
		return shim.Error("This user already exists: " + new_name)
	}
	userAddressAsBytes, err := stub.GetState(UserPrefix + new_add)
	if err != nil {
		return shim.Error("Fail to get user by address: " + err.Error())
	} else if userAddressAsBytes != nil {
		return shim.Error("This address already registered")
	}

	// register user
	user := &user{new_name, new_intro, new_add, 1, 0, 0, 0}
	userJSONasBytes, err := json.Marshal(user)
	if err != nil {
		return shim.Error(err.Error())
	}
	err = stub.PutState(user_key, userJSONasBytes)
	if err != nil {
		return shim.Error(err.Error())
	}
	err = stub.PutState(UserPrefix+new_add, userJSONasBytes)
	if err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success([]byte("User register success."))
}

// ===================================
// removeUser: Remove an existed user
// ===================================
func (t *serviceChaincode) removeUser(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var user_name, new_add string
	var err error

	user_name = args[0]

	// Get the user's address automatically through INKchian's GetSender() interface
	new_add, err = stub.GetSender()
	if err != nil {
		return shim.Error("Fail to get the sender's address.")
	}

	// check if user exists
	user_key := UserPrefix + user_name
	userAsBytes, err := stub.GetState(user_key)
	if err != nil {
		return shim.Error("Fail to get user: " + err.Error())
	} else if userAsBytes == nil {
		return shim.Error("This user does not exist: " + user_name)
	}

	err = stub.DelState(user_key)
	if err != nil {
		return shim.Error(err.Error())
	}
	err = stub.DelState(UserPrefix + new_add)
	if err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success([]byte("User delete success."))
}

// ===================================
// queryUser: Query an existed user
// ===================================
func (t *serviceChaincode) queryUser(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var user_name string
	var err error

	user_name = args[0]

	// check if user exists
	user_key := UserPrefix + user_name
	userAsBytes, err := stub.GetState(user_key)
	if err != nil {
		return shim.Error("Fail to get user: " + err.Error())
	} else if userAsBytes == nil {
		return shim.Error("This user does not exist: " + user_name)
	}
	var userJson user
	err = json.Unmarshal(userAsBytes, &userJson)
	if err != nil {
		return shim.Error(err.Error())
	}
	userJson = t.calcContribution(userJson)
	userAsBytes, err = json.Marshal(userJson)
	if err != nil {
		return shim.Error(err.Error())
	}
	// return user info
	return shim.Success(userAsBytes)
}

// Invoke func about service
// ==================================================================================

// =======================================
// registerService: Register a new service
// =======================================
func (t *serviceChaincode) registerService(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var service_name string
	var service_type string
	var service_des string
	var service_dev string
	var user_name string
	var service_address string
	var price *big.Int
	var err error

	service_name = args[0]
	service_type = args[1]
	service_des = args[2]
	user_name = args[3]
	service_address = args[4]
	priceStr := args[5]
	price, ok := big.NewInt(0).SetString(priceStr, 10)
	if !ok {
		return shim.Error("6th args must be intefer")
	}

	// get service developer, check if it corresponds with the input user
	service_dev, err = stub.GetSender()
	if err != nil {
		return shim.Error("Fail to get the sender's address.")
	}
	user_key := UserPrefix + user_name
	userAsBytes, err := stub.GetState(user_key)
	if err != nil {
		return shim.Error("Fail to get user: " + err.Error())
	}
	var userJSON user
	err = json.Unmarshal([]byte(userAsBytes), &userJSON)
	if err != nil {
		return shim.Error("Error unmarshal user bytes.")
	}
	if userJSON.Address != service_dev {
		return shim.Error("Not the correct user.")
	}

	// check if service exists
	service_key := ServicePrefix + service_name
	serviceAsBytes, err := stub.GetState(service_key)
	if err != nil {
		return shim.Error("Fail to get service: " + err.Error())
	} else if serviceAsBytes != nil {
		return shim.Error("This service already exists: " + service_name)
	}

	// get current time
	tNow := time.Now()
	tString := tNow.UTC().Format(time.UnixDate)

	// register service
	newS := &service{service_name, service_type, user_name,
		service_des, service_address, price, tString, "", S_Created,
		false, make(map[string]int)}
	serviceJSONasBytes, err := json.Marshal(newS)
	if err != nil {
		return shim.Error(err.Error())
	}
	err = stub.PutState(service_key, serviceJSONasBytes)
	if err != nil {
		return shim.Error(err.Error())
	}
	err = t.saveServiceByUserName(stub, user_name, service_name, serviceJSONasBytes)
	userJSON.TotalService = userJSON.TotalService + 1
	err = t.updateUser(userJSON, stub)
	if err != nil {
		return shim.Error(err.Error())
	}
	return shim.Success([]byte("Service register success."))
}

// =================================================
// invalidateService: Invalidate an existed service
// =================================================
func (t *serviceChaincode) invalidateService(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var service_name string
	var err error

	service_name = args[0]

	// STEP 0: check if service exists
	service_key := ServicePrefix + service_name
	serviceAsBytes, err := stub.GetState(service_key)
	if err != nil {
		return shim.Error("Fail to get service: " + err.Error())
	} else if serviceAsBytes == nil {
		return shim.Error("This service does not exists: " + service_name)
	}

	// STEP 1: check whether it is the service's developer's invocation
	var senderAdd string
	senderAdd, err = stub.GetSender()
	if err != nil {
		return shim.Error("Fail to get the sender's address.")
	}

	var serviceJSON service
	err = json.Unmarshal([]byte(serviceAsBytes), &serviceJSON)
	if err != nil {
		return shim.Error("Error unmarshal service bytes.")
	}

	// 0125
	// get developer's address
	dev_key := UserPrefix + serviceJSON.Developer
	devAsBytes, err := stub.GetState(dev_key)
	if err != nil {
		return shim.Error("Error get the developer.")
	}
	var DevJSON user
	err = json.Unmarshal([]byte(devAsBytes), &DevJSON)

	fmt.Println("DevAddress:  " + DevJSON.Address)
	if senderAdd != DevJSON.Address {
		return shim.Error("Aurthority err! Not invoke by the service's developer.")
	}

	// STEP 2: invalidate the service and store it.
	// new service, make it invalidated
	new_service := &service{serviceJSON.Name, serviceJSON.Type, serviceJSON.Developer,
		serviceJSON.Description, serviceJSON.Resource, serviceJSON.Price, serviceJSON.CreatedTime, serviceJSON.UpdatedTime,
		S_Invalid, serviceJSON.IsMashup, serviceJSON.Composition}
	// store the new service
	assetJSONasBytes, err := json.Marshal(new_service)
	if err != nil {
		return shim.Error(err.Error())
	}

	err = stub.PutState(service_key, assetJSONasBytes)
	if err != nil {
		return shim.Error(err.Error())
	}
	err = t.saveServiceByUserName(stub, new_service.Developer, service_name, assetJSONasBytes)

	return shim.Success([]byte("Invalidate Service success."))
}

// =================================================
// publishService: publish a created service
// =================================================
func (t *serviceChaincode) publishService(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var service_name string
	var err error

	service_name = args[0]

	// STEP 0: check if service exists
	service_key := ServicePrefix + service_name
	serviceAsBytes, err := stub.GetState(service_key)
	if err != nil {
		return shim.Error("Fail to get service: " + err.Error())
	} else if serviceAsBytes == nil {
		return shim.Error("This service does not exists: " + service_name)
	}

	// STEP 1: check whether it is the service's developer's invocation
	var senderAdd string
	senderAdd, err = stub.GetSender()
	if err != nil {
		return shim.Error("Fail to get the sender's address.")
	}

	var serviceJSON service
	err = json.Unmarshal([]byte(serviceAsBytes), &serviceJSON)
	if err != nil {
		return shim.Error("Error unmarshal service bytes.")
	}

	fmt.Println("SenderAdd:  " + senderAdd)
	fmt.Println("Developer:  " + serviceJSON.Developer)

	// 0125
	// get developer's address
	dev_key := UserPrefix + serviceJSON.Developer
	devAsBytes, err := stub.GetState(dev_key)
	if err != nil {
		return shim.Error("Error get the developer.")
	}
	var DevJSON user
	err = json.Unmarshal([]byte(devAsBytes), &DevJSON)

	fmt.Println("DevAddress:  " + DevJSON.Address)
	if senderAdd != DevJSON.Address {
		return shim.Error("Aurthority err! Not invoke by the service's developer.")
	}

	// STEP 2: publish the service and store it.
	// new service, make it invalidated
	new_service := &service{serviceJSON.Name, serviceJSON.Type, serviceJSON.Developer,
		serviceJSON.Description, serviceJSON.Resource, serviceJSON.Price, serviceJSON.CreatedTime, serviceJSON.UpdatedTime,
		S_Available, serviceJSON.IsMashup, serviceJSON.Composition}
	// store the new service
	serviceJSONasBytes, err := json.Marshal(new_service)
	if err != nil {
		return shim.Error(err.Error())
	}

	err = stub.PutState(service_key, serviceJSONasBytes)
	if err != nil {
		return shim.Error(err.Error())
	}
	err = t.saveServiceByUserName(stub, new_service.Developer, service_name, serviceJSONasBytes)

	return shim.Success([]byte("Publish Service success."))
}

// ======================================
// queryService: Query an existed service
// ======================================
func (t *serviceChaincode) queryService(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var service_name string
	var err error

	service_name = args[0]

	// check if service exists
	service_key := ServicePrefix + service_name
	serviceAsBytes, err := stub.GetState(service_key)
	if err != nil {
		return shim.Error("Fail to get service: " + err.Error())
	} else if serviceAsBytes == nil {
		return shim.Error("This service does not exist: " + service_name)
	}

	// return service info
	return shim.Success(serviceAsBytes)
}

// ======================================
// editService: Edit an existed service
// args[0]: service name
// args[1]: service type
// args[2]: service description
// args[3]: service path
// args[4]: service price
// ======================================
func (t *serviceChaincode) editService(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var serviceName, serviceType, description, resource, priceStr string
	var price *big.Int
	var err error

	serviceName = args[0]
	serviceType = args[1]
	description = args[2]
	resource = args[3]
	priceStr = args[4]
	price, ok := big.NewInt(0).SetString(priceStr, 10)
	if !ok {
		return shim.Error("5th args must be intefer")
	}

	// STEP 0: check the service does not exist
	serviceKey := ServicePrefix + serviceName
	serviceAsBytes, err := stub.GetState(serviceKey)
	if err != nil {
		return shim.Error("Fail to get service: " + err.Error())
	} else if serviceAsBytes == nil {
		return shim.Error("This service does not exist: " + serviceName)
	}

	// STEP 1: check whether it is the service's developer's invocation
	var senderAdd string
	senderAdd, err = stub.GetSender()
	if err != nil {
		return shim.Error("Fail to get the sender's address.")
	}

	var serviceJSON service
	err = json.Unmarshal([]byte(serviceAsBytes), &serviceJSON)
	if err != nil {
		return shim.Error("Error unmarshal service bytes.")
	}

	// 0125
	// get developer's address
	devKey := UserPrefix + serviceJSON.Developer
	devAsBytes, err := stub.GetState(devKey)
	if err != nil {
		return shim.Error("Error get the developer.")
	}
	var DevJSON user
	err = json.Unmarshal([]byte(devAsBytes), &DevJSON)

	if senderAdd != DevJSON.Address {
		return shim.Error("Aurthority err! Not invoke by the service's developer.")
	}

	// STEP 2: update time information
	tNow := time.Now()
	tString := tNow.UTC().Format(time.UnixDate)

	newService := &service{serviceJSON.Name, serviceType, serviceJSON.Developer,
		description, resource, price, serviceJSON.CreatedTime, tString,
		serviceJSON.Status, serviceJSON.IsMashup, serviceJSON.Composition}
	// STEP 4: store the service
	serviceJSONasBytes, err := json.Marshal(newService)
	if err != nil {
		return shim.Error(err.Error())
	}

	err = stub.PutState(serviceKey, serviceJSONasBytes)
	if err != nil {
		return shim.Error(err.Error())
	}
	err = t.saveServiceByUserName(stub, newService.Developer, serviceName, serviceJSONasBytes)

	// return service info
	return shim.Success(serviceAsBytes)
}

// =======================================================
// createMashup: Create a new mashup
// note: a mashup should invoke at least one service API
// =======================================================
func (t *serviceChaincode) createMashup(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var mashup_name string
	var mashup_type string
	var mashup_des string
	var mashup_dev string
	var user_name string
	var price *big.Int
	var err error

	mashup_name = args[0]
	mashup_type = args[1]
	mashup_des = args[2]
	user_name = args[3]
	price_str := args[4]
	price, ok := big.NewInt(0).SetString(price_str, 10)
	if !ok {
		return shim.Error("4th args must be integer")
	}

	// STEP 0: get mashup developer
	mashup_dev, err = stub.GetSender()
	if err != nil {
		return shim.Error("Fail to get the sender's address.")
	}
	user_key := UserPrefix + user_name
	userAsBytes, err := stub.GetState(user_key)
	if err != nil {
		return shim.Error("Fail to get user: " + err.Error())
	}
	var userJSON user
	err = json.Unmarshal([]byte(userAsBytes), &userJSON)
	if err != nil {
		return shim.Error("Error unmarshal user bytes.")
	}
	if userJSON.Address != mashup_dev {
		return shim.Error("Not the correct user.")
	}

	// STEP 1: check if service does not exist
	mashup_key := ServicePrefix + mashup_name
	serviceAsBytes, err := stub.GetState(mashup_key)
	if err != nil {
		return shim.Error("Fail to get service: " + err.Error())
	} else if serviceAsBytes != nil {
		return shim.Error("This service already exists: " + mashup_name)
	}

	// STEP 2: create a new mashup
	// get current time
	tNow := time.Now()
	tString := tNow.UTC().Format(time.UnixDate)

	// create composition
	new_map := make(map[string]int)
	new_developer_map := make(map[string]int)
	for i := 5; i < len(args); i++ {
		// check the service exist
		service_key := ServicePrefix + args[i]
		serviceAsBytes, err := stub.GetState(service_key)
		if err != nil {
			return shim.Error("Fail to get service: " + err.Error())
		} else if serviceAsBytes == nil {
			return shim.Error("This service doesn't exist: " + args[i])
		}
		// add the service into map
		new_map[args[i]] = 1
		// temporarily store their addresses
		var serviceJSON service
		err = json.Unmarshal([]byte(serviceAsBytes), &serviceJSON)
		if err != nil {
			return shim.Error("Error unmarshal service bytes.")
		}
		new_developer_map[serviceJSON.Developer] = 1
	}

	// new mashup
	newS := &service{mashup_name, mashup_type, user_name,
		mashup_des, "", price, tString, "", S_Created,
		true, new_map}

	// STEP 3: pay to the invoked services' developers
	// Important!
	// Incentive Mechanism Here

	incentive_amount := big.NewInt(0)
	incentive_amount.SetString(IncentiveMashupInvoke, 10)

	for k, _ := range (new_developer_map) {
		// get the k's address
		user_key := UserPrefix + k
		userAsBytes, err := stub.GetState(user_key)
		if err != nil {
			return shim.Error("Fail to get user: " + err.Error())
		} else if userAsBytes == nil {
			return shim.Error("This user doesn't exist: " + k)
		}
		var userJSON user
		err = json.Unmarshal([]byte(userAsBytes), &userJSON)
		if err != nil {
			return shim.Error("Error unmarshal user bytes.")
		}
		// make incentive transfer
		// from the mashup developer to the invoked service's developer
		err = stub.Transfer(userJSON.Address, FeeBalanceType, incentive_amount)
		if err != nil {
			return shim.Error("Error when making transfer.")
		}
	}

	// STEP 4: store the new mashup
	serviceJSONasBytes, err := json.Marshal(newS)
	if err != nil {
		return shim.Error(err.Error())
	}
	err = stub.PutState(mashup_key, serviceJSONasBytes)
	if err != nil {
		return shim.Error(err.Error())
	}
	err = t.saveServiceByUserName(stub, user_name, mashup_name, serviceJSONasBytes)
	userJSON.TotalService = userJSON.TotalService + 1
	err = t.updateUser(userJSON, stub)
	if err != nil {
		return shim.Error(err.Error())
	}
	return shim.Success([]byte("Mashup register success."))
}

// =======================================================
// rewardService: reward a service
// reward a service's developer, transfer fixed amount of
// specific reward_type token to the developer's account.
// =======================================================
func (t *serviceChaincode) rewardService(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var service_name string
	var reward_type string
	var err error

	service_name = args[0]
	reward_type = args[1]

	// Amount
	reward_amount := big.NewInt(0)
	_, good := reward_amount.SetString(args[2], 10)
	if !good {
		return shim.Error("Expecting integer value for amount")
	}

	// STEP 0: get service's developer
	service_key := ServicePrefix + service_name
	serviceAsBytes, err := stub.GetState(service_key)
	if err != nil {
		return shim.Error("Fail to get the service's info.")
	}

	var serviceJSON service
	err = json.Unmarshal([]byte(serviceAsBytes), &serviceJSON)
	if err != nil {
		return shim.Error("Error unmarshal service bytes.")
	}

	dev := serviceJSON.Developer

	// STEP 1: get the address of the dev
	user_key := UserPrefix + dev
	userAsBytes, err := stub.GetState(user_key)
	if err != nil {
		return shim.Error("Fail to get the developer's info.")
	}
	var userJSON user
	err = json.Unmarshal([]byte(userAsBytes), &userJSON)
	if err != nil {
		return shim.Error("Error unmarshal user bytes.")
	}

	// STEP 3: reward the developer
	toAdd := userJSON.Address
	err = stub.Transfer(toAdd, reward_type, reward_amount)
	if err != nil {
		return shim.Error("Fail realize the reawrd.")
	}

	return shim.Success([]byte("Reward the service success."))
}

// ========================================================================
// queryServiceByRange: query services' by page and limit
//
// // page and limit are case-se
// ========================================================================
func (t *serviceChaincode) queryServiceByRange(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var page, limit int64
	var err error
	page, err = strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return shim.Error(err.Error())
	}
	limit, err = strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return shim.Error(err.Error())
	}
	if limit == 0 {
		limit = 10
	}
	if page <= 0 {
		page = 1
	}
	start := (page - 1) * limit
	resultsIterator, err := stub.GetStateByPartialCompositeKey(UserServicesKey, []string{})
	if err != nil {
		return shim.Error(err.Error())
	}
	services := make([]*service, 0)
	for i := int64(0); resultsIterator.HasNext(); i++ {
		responseRange, err := resultsIterator.Next()
		if err != nil {
			return shim.Error(err.Error())
		}
		if i >= start && i < start+limit {
			service := &service{}
			err = json.Unmarshal(responseRange.Value, service)
			if err != nil {
				return shim.Error(err.Error())
			}
			services = append(services, service)
		} else if i >= start+limit {
			break
		}
	}
	servicesBytes, err := json.Marshal(services)
	if err != nil {
		return shim.Error(err.Error())
	}
	return shim.Success(servicesBytes)
}

// ========================================================================
// saveServiceByUserName: save service with key which include user name and service name
//
// userName are required
// serviceName are required
// ========================================================================
func (t *serviceChaincode) saveServiceByUserName(stub shim.ChaincodeStubInterface, userName string, serviceName string, state []byte) error {
	compositeKey, err := stub.CreateCompositeKey(UserServicesKey, []string{userName, serviceName})
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("create composite key error: %s", err.Error()))
	}
	err = stub.PutState(compositeKey, state)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("save error: %s", err.Error()))
	}
	return nil
}

// ========================================================================
// queryServiceByUser: query services' names by user name (name)
//
// name are case-sensitive
// use "" for both name if you want to query all the assets
// ========================================================================
func (t *serviceChaincode) queryServiceByUser(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var page, limit int64
	var err error
	page, err = strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return shim.Error(err.Error())
	}
	limit, err = strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return shim.Error(err.Error())
	}
	if limit == 0 {
		limit = 10
	}
	if page <= 0 {
		page = 1
	}
	start := (page - 1) * limit
	resultsIterator, err := stub.GetStateByPartialCompositeKey(UserServicesKey, []string{args[2]})
	if err != nil {
		return shim.Error(err.Error())
	}
	services := make([]*service, 0)
	for i := int64(0); resultsIterator.HasNext(); i++ {
		responseRange, err := resultsIterator.Next()
		if err != nil {
			return shim.Error(err.Error())
		}
		if i >= start && i < start+limit {
			service := &service{}
			err = json.Unmarshal(responseRange.Value, service)
			if err != nil {
				return shim.Error(err.Error())
			}
			services = append(services, service)
		} else if i >= start+limit {
			break
		}
	}
	servicesBytes, err := json.Marshal(services)
	if err != nil {
		return shim.Error(err.Error())
	}
	return shim.Success(servicesBytes)
}

// ========================================================================
// callService: pay tokens to buy one service call times
//
// serviceName and callTimes are required
// ========================================================================
func (t *serviceChaincode) callService(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var service_name, sender string
	call_times := big.NewInt(0)
	total := big.NewInt(0)
	var service_data service
	var user_data user
	var record serviceCallTime
	var err error

	time_stamp, err := stub.GetTxTimestamp()
	if err != nil {
		return shim.Error("Can't get timestamp : " + err.Error())
	}

	sender, err = stub.GetSender()
	if err != nil {
		return shim.Error("Failed to get sender : " + err.Error())
	}

	service_name = strings.TrimSpace(args[0])
	if len(service_name) <= 0 {
		return shim.Error("1st arg must be non-empty string")
	}
	call_time_str := strings.TrimSpace(args[1])
	call_times, ok := big.NewInt(0).SetString(call_time_str, 10)
	if !ok {
		return shim.Error("2th arg must be integer")
	}

	userAsJson, err := stub.GetState(UserPrefix + sender)
	if err != nil {
		return shim.Error("Get user info failed: " + err.Error())
	} else if userAsJson == nil {
		return shim.Error("User not registered")
	}
	err = json.Unmarshal(userAsJson, &user_data)
	if err != nil {
		return shim.Error("Unmarshal user info failed: " + err.Error())
	}

	serviceJson, err := stub.GetState(ServicePrefix + service_name)
	if err != nil {
		return shim.Error("Get service info failed : " + err.Error())
	} else if serviceJson == nil {
		return shim.Error("Service not exists")
	}
	service_data = service{}
	err = json.Unmarshal(serviceJson, &service_data)
	if err != nil {
		return shim.Error("Unmarshal service info failed: " + err.Error())
	}
	if service_data.Status != S_Available {
		return shim.Error("Service not invalid")
	}

	total = total.Mul(service_data.Price, call_times)
	record_key := ServiceCallTimesPrefix + service_name + user_data.Name
	callTimesJson, err := stub.GetState(record_key)
	if err != nil {
		return shim.Error("Get old call times log failed: " + err.Error())
	} else if callTimesJson != nil {
		err = json.Unmarshal(callTimesJson, &record)
		if err != nil {
			return shim.Error("Unmarshal old call times log failed: " + err.Error())
		}
		record.CallTimes = big.NewInt(0).Add(call_times, record.CallTimes)
		record.UpdateTime = time_stamp.String()
		record.Total = big.NewInt(0).Add(total, record.Total)
	} else {
		record = serviceCallTime{service_name, user_data.Name, sender, call_times, total, time_stamp.String(), time_stamp.String()}
	}

	recordJson, err := json.Marshal(record)
	if err != nil {
		return shim.Error("Marshal call time info failed: " + err.Error())
	}
	developerKey := UserPrefix + service_data.Developer
	developerAsBytes, err := stub.GetState(developerKey)
	if err != nil {
		return shim.Error("Fail to get the developer's info.")
	}
	var developer user
	err = json.Unmarshal([]byte(developerAsBytes), &developer)
	if err != nil {
		return shim.Error("Error unmarshal developer bytes.")
	}

	err = stub.Transfer(developer.Address, FeeBalanceType, service_data.Price)
	if err != nil {
		return shim.Error("Send service fee failed: " + err.Error())
	}
	err = stub.PutState(record_key, recordJson)
	if err != nil {
		return shim.Error("Failed to save call time info: " + err.Error())
	}

	buy_record := buyRecord{record_key, service_name, user_data.Name, call_times, total, time_stamp.String()}
	buyRecordJson, err := json.Marshal(buy_record)
	if err != nil {
		return shim.Error("Marshal buy record failed:" + err.Error())
	}
	buy_record_key := fmt.Sprintf("%s%s%s%d", BuyRecordPrefix, service_name, user_data.Name, time_stamp.Seconds)
	err = stub.PutState(buy_record_key, buyRecordJson)
	if err != nil {
		return shim.Error("Save buy record failed: " + err.Error())
	}
	err = t.saveCallTimesByServiceName(stub, service_name, record_key, recordJson)
	user_data.TotalCallTimes = user_data.TotalCallTimes + int(call_times.Int64())
	err = t.updateUser(user_data, stub)
	if err != nil {
		return shim.Error(err.Error())
	}
	return shim.Success(nil)
}

// ========================================================================
// saveCallTimesByServiceName: save callTime record with key which include service name and call time key
//
// serviceName are required
// recordKey are required
// ========================================================================
func (t *serviceChaincode) saveCallTimesByServiceName(stub shim.ChaincodeStubInterface, serviceName string, recordKey string, state []byte) error {
	compositeKey, err := stub.CreateCompositeKey(CallTimeKey, []string{serviceName, recordKey})
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("create composite key error: %s", err.Error()))
	}
	err = stub.PutState(compositeKey, state)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("save error: %s", err.Error()))
	}
	return nil
}

// ========================================================================
// getCallTimes: query callTimes by service name
//
// name are case-sensitive
// use "" for both name if you want to query all the assets
// ========================================================================
func (t *serviceChaincode) getCallTimes(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var err error
	resultsIterator, err := stub.GetStateByPartialCompositeKey(CallTimeKey, args)
	if err != nil {
		return shim.Error(err.Error())
	}
	callTimes := make([]*serviceCallTime, 0)
	for i := 0; resultsIterator.HasNext(); i++ {
		responseRange, err := resultsIterator.Next()
		if err != nil {
			return shim.Error(err.Error())
		}
		callTime := &serviceCallTime{}
		err = json.Unmarshal(responseRange.Value, callTime)
		if err != nil {
			return shim.Error(err.Error())
		}
		callTimes = append(callTimes, callTime)
	}
	callTimesBytes, err := json.Marshal(callTimes)
	if err != nil {
		return shim.Error(err.Error())
	}
	return shim.Success(callTimesBytes)
}

// ========================================================================
// getCallTime: query some one has buy service's call times
//
// serviceName and userName are required
// ========================================================================
func (t *serviceChaincode) getCallTime(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var service_name, user_name string
	var err error

	service_name = strings.TrimSpace(args[0])
	if len(service_name) <= 0 {
		return shim.Error("1st arg must be non-empty string")
	}

	user_name = strings.TrimSpace(args[1])
	if len(user_name) <= 0 {
		return shim.Error("2st arg must be non-empty string")
	}

	record_key := ServiceCallTimesPrefix + service_name + user_name
	callTimeJson, err := stub.GetState(record_key)
	if err != nil {
		return shim.Error(err.Error())
	} else if callTimeJson == nil {
		return shim.Error("User " + user_name + " have never buy service call times")
	}
	return shim.Success(callTimeJson)
}

// ========================================================================
// reduceCallTime: reduce user's service call times
//
// serviceName, userName and reduceTime are required
// ========================================================================
func (t *serviceChaincode) reduceCallTime(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var service_name, sender, caller string
	var reduce_time *big.Int
	var call_time serviceCallTime
	var reduce_record reduceRecord
	var service_data service
	var user_data user
	var err error

	time_stamp, err := stub.GetTxTimestamp()
	if err != nil {
		return shim.Error("Can't get timestamp : " + err.Error())
	}

	sender, err = stub.GetSender()
	if err != nil {
		return shim.Error("Failed to get sender : " + err.Error())
	}

	service_name = strings.TrimSpace(args[0])
	if len(service_name) == 0 {
		return shim.Error("1st arg must be non-empty string")
	}
	caller = strings.TrimSpace(args[1])
	if len(caller) == 0 {
		return shim.Error("2st arg must be non-empty string")
	}
	reduce_time_str := strings.TrimSpace(args[2])
	reduce_time, ok := big.NewInt(0).SetString(reduce_time_str, 10)
	if !ok {
		return shim.Error("3th arg must be integer")
	}

	userAsJson, err := stub.GetState(UserPrefix + sender)
	if err != nil {
		return shim.Error("Get user info failed: " + err.Error())
	} else if userAsJson == nil {
		return shim.Error("User not registered")
	}
	err = json.Unmarshal(userAsJson, &user_data)
	if err != nil {
		return shim.Error("Unmarshal user info failed: " + err.Error())
	}

	service_key := ServicePrefix + service_name
	serviceAsBytes, err := stub.GetState(service_key)
	if err != nil {
		return shim.Error("Fail to get service: " + err.Error())
	} else if serviceAsBytes == nil {
		return shim.Error("This service does not exist: " + service_name)
	}
	err = json.Unmarshal(serviceAsBytes, &service_data)
	if err != nil {
		return shim.Error("Fail to unmarshal service data")
	} else if service_data.Developer != user_data.Name {
		return shim.Error("Service not developed by you")
	}

	call_time_key := ServiceCallTimesPrefix + service_name + caller
	callTimeJson, err := stub.GetState(call_time_key)
	if err != nil {
		return shim.Error("Get call time info failed : " + err.Error())
	} else if callTimeJson == nil {
		return shim.Error("Have not buy this service call time")
	}
	err = json.Unmarshal(callTimeJson, &call_time)
	if err != nil {
		return shim.Error("Unmarshal call time info failed : " + err.Error())
	}

	if call_time.CallTimes.Cmp(big.NewInt(0)) == 0 && call_time.CallTimes.Cmp(reduce_time) < 0 {
		return shim.Error("Have not enough call times")
	}
	call_time.CallTimes = call_time.CallTimes.Sub(call_time.CallTimes, reduce_time)
	call_time.UpdateTime = time_stamp.String()
	callTimeJson, err = json.Marshal(call_time)
	if err != nil {
		return shim.Error("Marshal call time info failed : " + err.Error())
	}
	err = stub.PutState(call_time_key, callTimeJson)
	if err != nil {
		return shim.Error("Update call time failed : " + err.Error())
	}

	reduce_key := fmt.Sprintf("%s%s%s%d", ReduceRecordPrefix, service_name, caller, time_stamp.Seconds)
	reduce_record = reduceRecord{service_name, call_time_key, user_data.Name, reduce_time, time_stamp.String()}
	reduceJson, err := json.Marshal(reduce_record)
	if err != nil {
		return shim.Error("Marshal reduce info failed : " + err.Error())
	}

	err = stub.PutState(reduce_key, reduceJson)
	if err != nil {
		return shim.Error("Save reduce info failed : " + err.Error())
	}
	user_data.TotalInvokeTimes = user_data.TotalInvokeTimes + int(reduce_time.Int64())
	err = t.updateUser(user_data, stub)
	if err != nil {
		return shim.Error(err.Error())
	}
	return shim.Success(nil)
}

func (t *serviceChaincode) calcContribution(serviceUser user) user {
	totalService := float64(serviceUser.TotalService)
	totalInvokeTimes := float64(serviceUser.TotalInvokeTimes)
	totalCallTimes := float64(serviceUser.TotalCallTimes)
	if totalService == 0 {
		serviceUser.Contribution = math.Log(totalService + 1)
	} else {
		serviceUser.Contribution = math.Log(totalService+1) +  L*(totalInvokeTimes/totalService) + R*(totalCallTimes/totalService)
	}
	return serviceUser
}

func (t *serviceChaincode) updateUser(serviceUser user, stub shim.ChaincodeStubInterface) error {
	userKey := UserPrefix + serviceUser.Name
	userJSONasBytes, err := json.Marshal(serviceUser)
	if err != nil {
		return err
	}
	err = stub.PutState(userKey, userJSONasBytes)
	if err != nil {
		return err
	}
	err = stub.PutState(UserPrefix+serviceUser.Address, userJSONasBytes)
	if err != nil {
		return err
	}
	return nil
}
