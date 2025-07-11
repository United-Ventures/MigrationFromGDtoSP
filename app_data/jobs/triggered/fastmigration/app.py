from flask import Flask, request, jsonify
from sharepoint_drive import SharePointDrive

app = Flask(__name__)
drive = SharePointDrive()

# --- pulsante già esistente -----------------------------------------------
@app.route("/buttonZohoDeals", methods=["POST"])
def buttonZohoDeals():
    try:
        data = request.get_json(force=True)
        print("✅ RICEVUTO AddFiles:", data)
        record_id = int(data["zohoId"])
        url = drive.buttonZohoDeals(record_id)
        print("📁 URL AddFiles:", url)
        return jsonify({"url": url})
    except Exception as e:
        print("❌ ERRORE AddFiles:", e)
        return jsonify({"error": str(e)}), 500

@app.route("/buttonZohoBusinessReview", methods=["POST"])
def buttonZohoBusinessReview():
    try:
        data = request.get_json(force=True)
        print("✅ RICEVUTO AddMonitoring:", data)
        record_id = int(data["zohoId"])
        url = drive.buttonZohoBusinessReview(record_id)
        print("📁 URL Monitoring:", url)
        return jsonify({"url": url})
    except Exception as e:
        print("❌ ERRORE Monitoring:", e)
        return jsonify({"error": str(e)}), 500
    
@app.route("/buttonZohoRound", methods=["POST"])
def buttonZohoRound():
    try:
        data = request.get_json(force=True)
        print("✅ RICEVUTO AddMonitoring:", data)
        record_id = int(data["zohoId"])
        url = drive.buttonZohoRound(record_id)
        print("📁 URL Monitoring:", url)
        return jsonify({"url": url})
    except Exception as e:
        print("❌ ERRORE Monitoring:", e)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(port=5100, debug=True)
