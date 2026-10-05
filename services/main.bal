import ballerina/http;
import ballerina/time;
import ballerinax/mongodb;
import ballerinax/kafka;

listener http:Listener apiListener = new (8080);
configurable string mongoUri = "mongodb://foodapp:foodapp-local@mongodb:27017/?authSource=admin";
configurable string kafkaUrl = "redpanda:9092";

type MenuItem record {| string id; string name; string description; decimal price; int stock; |};
type Restaurant record {| string id; string name; string cuisine; string eta; string rating; string color; string hours; boolean isOpen; MenuItem[] menu; |};
type FoodOrder record {| string id; string customer; string customerId; string address; string restaurantId; string restaurantName; MenuItem[] items; decimal total; string status; string createdAt; anydata...; |};
type StoredDocument record {| anydata...; |};
type Customer record {| string id; string name; string email; string phone; string createdAt; anydata...; |};
type Payment record {| string id; string orderId; decimal amount; string currency; string status; string providerReference; string createdAt; anydata...; |};
type Delivery record {| string id; string orderId; string driverName; string status; string createdAt; anydata...; |};

final Restaurant[] restaurants = [
    {id: "savanna", name: "Savanna Kitchen", cuisine: "Namibian · Grill", eta: "25–35 min", rating: "4.9", color: "#d77d42", hours: "09:00–22:00", isOpen: true, menu: [
        {id: "kapana", name: "Kapana Bowl", description: "Chargrilled beef, oshifima, tomato salsa", price: 98.00, stock: 30},
        {id: "potjie", name: "Sunday Potjie", description: "Slow-cooked seasonal vegetables and beef", price: 125.00, stock: 15},
        {id: "vetkoek", name: "Vetkoek & Mince", description: "Warm vetkoek filled with spiced mince", price: 72.00, stock: 25}
    ]},
    {id: "coast", name: "Coast & Crumb", cuisine: "Seafood · Café", eta: "20–30 min", rating: "4.8", color: "#5596a2", hours: "08:00–21:00", isOpen: true, menu: [
        {id: "fish", name: "Atlantic Fish Plate", description: "Pan-seared hake, lemon, herb potatoes", price: 145.00, stock: 18},
        {id: "toastie", name: "Smoked Snoek Toastie", description: "Local snoek, cheddar, sourdough", price: 89.00, stock: 20},
        {id: "tart", name: "Lemon Milk Tart", description: "A bright, creamy Namibian classic", price: 48.00, stock: 32}
    ]},
    {id: "garden", name: "Garden Table", cuisine: "Fresh · Plant-based", eta: "30–40 min", rating: "4.7", color: "#748c62", hours: "10:00–20:00", isOpen: true, menu: [
        {id: "grain", name: "Desert Grain Bowl", description: "Pearl millet, roasted squash, greens", price: 105.00, stock: 20},
        {id: "wrap", name: "Market Garden Wrap", description: "Hummus, crisp vegetables, herb dressing", price: 82.00, stock: 28},
        {id: "juice", name: "Citrus Cooler", description: "Fresh orange, lime, a hint of ginger", price: 42.00, stock: 35}
    ]}
];

int nextOrder = 1001;

service / on apiListener {
    private final mongodb:Collection ordersCollection;
    private final mongodb:Collection restaurantsCollection;
    private final mongodb:Collection customersCollection;
    private final mongodb:Collection paymentsCollection;
    private final mongodb:Collection deliveriesCollection;
    private final mongodb:Collection notificationsCollection;
    private final kafka:Producer eventProducer;

    function init() returns error? {
        mongodb:Client mongoClient = check new ({connection: mongoUri});
        mongodb:Database database = check mongoClient->getDatabase("food_delivery");
        self.ordersCollection = check database->getCollection("orders");
        self.restaurantsCollection = check database->getCollection("restaurants");
        self.customersCollection = check database->getCollection("customers");
        self.paymentsCollection = check database->getCollection("payments");
        self.deliveriesCollection = check database->getCollection("deliveries");
        self.notificationsCollection = check database->getCollection("notifications");
        self.eventProducer = check new (kafkaUrl, {acks: kafka:ACKS_ALL, retryCount: 3, enableIdempotence: true});
        int restaurantCount = check self.restaurantsCollection->countDocuments({}, {});
        if restaurantCount == 0 {
            check self.restaurantsCollection->insertMany(restaurants);
        }
    }

    resource function get health() returns json => {status: "ok", "service": "food-delivery-api"};
    resource function get restaurants() returns json[]|error {
        stream<StoredDocument, error?> resultStream = check self.restaurantsCollection->find({}, {}, {"_id": 0}, StoredDocument);
        json[] result = [];
        StoredDocument[] documents = check from var document in resultStream select document;
        foreach StoredDocument document in documents {
            result.push(check document.cloneWithType(json));
        }
        check resultStream.close();
        return result;
    }

    resource function patch restaurants/[string restaurantId]/menu(@http:Payload json payload) returns http:Response|error {
        MenuItem[] menu = check payload.menu.ensureType();
        if menu.length() == 0 {
            http:Response invalid = new;
            invalid.statusCode = 400;
            invalid.setJsonPayload({"error": "A restaurant menu must have at least one item."});
            return invalid;
        }
        Restaurant? existing = check self.restaurantsCollection->findOne({id: restaurantId}, {}, {"_id": 0}, Restaurant);
        if existing is () {
            http:Response missing = new;
            missing.statusCode = 404;
            missing.setJsonPayload({"error": "Restaurant not found."});
            return missing;
        }
        mongodb:UpdateResult menuUpdate = check self.restaurantsCollection->updateOne({id: restaurantId}, {set: {menu: menu}}, {});
        check self.eventProducer->send({topic: "restaurants.menu.updated", key: restaurantId.toBytes(),
            value: {restaurantId: restaurantId, menu: menu, updatedAt: time:utcNow().toString()}});
        http:Response response = new;
        response.setJsonPayload({restaurantId: restaurantId, menu: menu});
        return response;
    }

    resource function get orders() returns json[]|error {
        stream<StoredDocument, error?> resultStream = check self.ordersCollection->find({}, {}, {"_id": 0}, StoredDocument);
        json[] result = [];
        StoredDocument[] documents = check from var document in resultStream select document;
        foreach StoredDocument document in documents {
            result.push(check document.cloneWithType(json));
        }
        check resultStream.close();
        return result;
    }

    resource function get customers/[string customerId]/orders() returns json[]|error {
        stream<StoredDocument, error?> resultStream = check self.ordersCollection->find({customerId: customerId}, {}, {"_id": 0}, StoredDocument);
        json[] result = [];
        StoredDocument[] documents = check from var document in resultStream select document;
        foreach StoredDocument document in documents { result.push(check document.cloneWithType(json)); }
        check resultStream.close();
        return result;
    }

 resource function post orders(@http:Payload json payload) returns http:Response|error {
        string customer = check payload.customer.ensureType();
        string customerId = check payload.customerId.ensureType();
        string address = check payload.address.ensureType();
        string restaurantId = check payload.restaurantId.ensureType();
        json[] requested = check payload.items.ensureType();
        Restaurant? restaurant = check self.restaurantsCollection->findOne({id: restaurantId}, {}, {"_id": 0}, Restaurant);
        if restaurant is () || requested.length() == 0 || customer.trim().length() == 0 || address.trim().length() == 0 {
            http:Response bad = new;
            bad.statusCode = 400;
            bad.setJsonPayload({"error": "A valid restaurant, customer, address, and at least one item are required."});
            return bad;
        }
        if !restaurant.isOpen {
            http:Response closed = new;
            closed.statusCode = 409;
            closed.setJsonPayload({"error": "This restaurant is currently closed."});
            return closed;
        }
        MenuItem[] selected = [];
        decimal total = 0;
        foreach json entry in requested {
            string itemId = check entry.id.ensureType();
            int quantity = check entry.quantity.ensureType();
            MenuItem? item = ();
            foreach MenuItem candidate in restaurant.menu {
                if candidate.id == itemId { item = candidate; }
            }
            if item is () || quantity < 1 || quantity > 20 || item.stock < quantity {
                http:Response invalid = new;
                invalid.statusCode = 400;
                invalid.setJsonPayload({"error": "Cart contains an invalid item or quantity."});
                return invalid;
            }
            int i = 0;
            while i < quantity { selected.push(item); i += 1; }
            total += item.price * quantity;
        }
        string orderId = "FD-" + time:utcNow().toString() + "-" + nextOrder.toString();
        nextOrder += 1;
        Customer? registeredCustomer = check self.customersCollection->findOne({id: customerId}, {}, {"_id": 0}, Customer);
        if registeredCustomer is () {
            http:Response invalidCustomer = new;
            invalidCustomer.statusCode = 400;
            invalidCustomer.setJsonPayload({"error": "Register as a customer before placing an order."});
            return invalidCustomer;
        }
        FoodOrder created = {id: orderId, customer: customer, customerId: customerId, address: address, restaurantId: restaurant.id,
            restaurantName: restaurant.name, items: selected, total: total, status: "CREATED",
            createdAt: time:utcNow().toString()};
        check self.ordersCollection->insertOne(created);
        int decrementedCount = 0;
        foreach json entry in requested {
            string itemId = check entry.id.ensureType();
            int quantity = check entry.quantity.ensureType();
            mongodb:UpdateResult stockUpdate = check self.restaurantsCollection->updateOne(
                {id: restaurantId, "menu.id": itemId, "menu.stock": {"$gte": quantity}},
                {inc: {"menu.$.stock": -quantity}}, {});
            if stockUpdate.modifiedCount == 0 {
                foreach int restoreIndex in 0 ..< decrementedCount {
                    string restoreId = check requested[restoreIndex].id.ensureType();
                    int restoreQuantity = check requested[restoreIndex].quantity.ensureType();
                    mongodb:UpdateResult restoreUpdate = check self.restaurantsCollection->updateOne(
                        {id: restaurantId, "menu.id": restoreId},
                        {inc: {"menu.$.stock": restoreQuantity}}, {});
                    _ = restoreUpdate;
                }
                mongodb:UpdateResult cancelOrderUpdate = check self.ordersCollection->updateOne(
                    {id: orderId}, {set: {status: "CANCELLED"}}, {});
                _ = cancelOrderUpdate;
                http:Response unavailable = new;
                unavailable.statusCode = 409;
                unavailable.setJsonPayload({"error": "The menu item sold out while you were checking out. Please refresh and try again."});
                return unavailable;
            }
            decrementedCount += 1;
        }
        check self.eventProducer->send({topic: "orders.created", key: orderId.toBytes(), value: created});
        http:Response response = new;
        response.statusCode = 201;
        response.setJsonPayload(check created.cloneWithType(json));
        return response;
    }

    resource function patch orders/[string orderId]/status(@http:Payload json payload) returns http:Response|error {
        string status = check payload.status.ensureType();
        string[] valid = ["CONFIRMED", "PREPARING", "READY", "OUT_FOR_DELIVERY", "DELIVERED", "CANCELLED"];
        if valid.indexOf(status) < 0 {
            http:Response invalid = new;
            invalid.statusCode = 400;
            invalid.setJsonPayload({"error": "Unsupported order status."});
            return invalid;
        }
            FoodOrder? existing = check self.ordersCollection->findOne({id: orderId}, {}, {"_id": 0}, FoodOrder);
            if existing is () {
                http:Response missing = new;
                missing.statusCode = 404;
                missing.setJsonPayload({"error": "Order not found."});
                return missing;
            }
            string? expected = ();
            if existing.status == "CREATED" { expected = "CONFIRMED"; }
            else if existing.status == "CONFIRMED" { expected = "PREPARING"; }
            else if existing.status == "PREPARING" { expected = "READY"; }
            else if existing.status == "READY" { expected = "OUT_FOR_DELIVERY"; }
            else if existing.status == "OUT_FOR_DELIVERY" { expected = "DELIVERED"; }
            if status != "CANCELLED" && status != expected {
                http:Response conflictResponse = new;
                conflictResponse.statusCode = 409;
                conflictResponse.setJsonPayload({"error": "Order status transition is not allowed."});
                return conflictResponse;
            }
            if existing.status == "DELIVERED" || existing.status == "CANCELLED" {
                http:Response conflictResponse = new;
                conflictResponse.statusCode = 409;
                conflictResponse.setJsonPayload({"error": "Completed or cancelled orders are final."});
                return conflictResponse;
            }
            string updatedAt = time:utcNow().toString();
            map<json> changes = {"status": status, "updatedAt": updatedAt};
            FoodOrder updated = {
                id: existing.id,
                customer: existing.customer,
                customerId: existing.customerId,
                address: existing.address,
                restaurantId: existing.restaurantId,
                restaurantName: existing.restaurantName,
                items: existing.items,
                total: existing.total,
                status: status,
                createdAt: existing.createdAt,
                "updatedAt": updatedAt
            };
            mongodb:UpdateResult _ = check self.ordersCollection->updateOne({id: orderId}, {set: changes}, {});
            if status == "CANCELLED" {
                foreach MenuItem orderedItem in existing.items {
                    mongodb:UpdateResult _ = check self.restaurantsCollection->updateOne({id: existing.restaurantId, "menu.id": orderedItem.id},
                        {inc: {"menu.$.stock": 1}}, {});
                }
            }
            check self.eventProducer->send({topic: "orders.status.changed", key: orderId.toBytes(), value: updated});
            http:Response response = new;
            response.setJsonPayload(check updated.cloneWithType(json));
            return response;
    }
