FROM ballerina/ballerina:2201.13.5
WORKDIR /home/ballerina
COPY . .
RUN bal build
EXPOSE 8080
CMD ["bal", "run", "target/bin/food_delivery_api.jar"]