function myFunction() {

    results.style.display = "none";
    loading.style.display = "block";

    var table = document.getElementById("tweets-table");

    var initial = '<tr><th>#Tag</th><th>Count</th><th>Avg.Sentiment</th></tr>'

    console.log("Fetching started....")

    fetch("/api")
        .then(res => res.json())
        .then(data => {

            results.style.display = "block";
            loading.style.display = "none";

            table.innerHTML = initial;

            for (let obj of data["data"]) {
                table.innerHTML += (`<tr><td>${obj.hashtags}</td><td>${obj.count}</td><td>${(Math.round(parseFloat(obj.sentiment.$numberDecimal) * 1000) / 1000).toFixed(3)}</td></tr>`)
            }

        })
        .catch(err => console.log(err))
}