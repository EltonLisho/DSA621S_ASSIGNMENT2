# DSA621S_ASSIGNMENT2
A local demonstration of the assignment's food-delivery brief: customer ordering UI, Ballerina REST API, MongoDB persistence, Kafka-compatible Redpanda events, and Docker Compose setup.

## Start from scratch

1. Install Docker Desktop with Docker Compose v2 and start it.
2. Copy `.env.example` to `.env` in this folder.
3. Open PowerShell in this folder and run:

   ```powershell
   docker compose up --build
   ```

4. Visit [http://localhost:5173](http://localhost:5173). The API is at [http://localhost:8080/health](http://localhost:8080/health).
5. Stop the environment with `Ctrl+C`, then `docker compose down`. Use `docker compose down --volumes` only when you want to erase the locally stored database and broker data.

Docker downloads the Ballerina, MongoDB, Redpanda, and Nginx images on first run. The API image resolves the pinned Ballerina connectors from Ballerina Central during its first build/run; internet access is required for that first dependency download.

## Included flows

- Browse restaurant menus, filter by cuisine, add items to the bag, and place orders.
- Orders are stored in MongoDB, and `orders.created` is published to Kafka.
- The payment consumer simulates payment, stores the result, updates the order, and publishes payment/status events.
- The delivery consumer assigns a demo driver when the restaurant marks an order ready.
- The notification consumer stores in-app notifications from lifecycle events.
- Customer, restaurant menu, payment, delivery, notification, and admin report API resources are included.
- The web interface polls for order status changes and supports lifecycle demonstration.

Payment and delivery are simulations for coursework. No card information is collected or stored.
