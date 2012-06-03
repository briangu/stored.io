echo "add red car"
curl -d@record.json http://localhost:8080/records
echo "add blue car"
curl -d@record2.json http://localhost:8080/records
echo "add tweet"
curl -d@tweet.json http://localhost:8080/records

