from flask import Flask, request, jsonify
from flask.ext import cors
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

app = Flask(__name__)
cors = cors.CORS(app)
try:
	db = MongoClient().hw5
	gradebook = db.gradebook
except ConnectionFailure as e:
	print e

cors_headers = {"Access-Control-Allow-Origin": "*"}

@app.route("/grade", methods=["POST"])
def grade_pages():
	grades = request.get_json(force=True)
	accessor = grades["accessor"]
	qid = grades["qid"]
	bulkWriter = gradebook.initialize_unordered_bulk_op()

	for grade in grades["grades"]:
		bulkWriter.insert({"qid": qid, "accessor": accessor,
			"docid": grade["url"], "grade": grade["score"]})

	try:
		bulkWriter.execute()
	except Exception, e:
		resp = jsonify(status="failed", reason=str(e))
		resp.status_code = 400
		return resp

	return jsonify(status="success", count=len(grades["grades"]))


if __name__ == '__main__':
	app.run("localhost", 8080, debug=True)