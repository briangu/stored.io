echo "add red car"
curl -d@src/main/resources/record.json http://localhost:8080/records
echo "add blue car"
curl -d@src/main/resources/record2.json http://localhost:8080/records
echo "add tweet"
curl -d@src/main/resources/tweet.json http://localhost:8080/records

