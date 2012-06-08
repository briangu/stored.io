echo "adding cars"
curl -d@src/main/resources/records.json http://localhost:8080/records
echo "add tweet"
curl -d@src/main/resources/tweet.json http://localhost:8080/records
