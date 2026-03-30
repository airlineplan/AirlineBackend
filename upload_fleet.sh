#!/bin/bash

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
API_URL="http://localhost:3000/fleet/bulk-save"

# Your JWT token
TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY5ODQ4NmE1YWU1ODQ3ODgxNDc1NDhhNiIsImVtYWlsIjoicCIsImlhdCI6MTc3NDgzNzYxOCwiZXhwIjoxNzc0ODYyODE4fQ.pHcFad2K3d5Dbqnjzl4uc5Jv0jhKAKoLCxOOB-4dq_s"

# ==========================================
# 🛠️ GENERATE JSON PAYLOAD
# ==========================================
echo "Generating Fleet Data..."

FLEET_ARRAY="["

for i in {1..27}; do
  NUM=$(printf "%02d" $i)
  REGN="VT-$NUM"
  SN="10$NUM" 
  ENTRY="2025-01-01T00:00:00.000Z"
  EXIT="2035-12-31T00:00:00.000Z" # 👈 NEW: Added an exit date far in the future
  
  ITEM=$(cat <<EOF
  {
    "category": "Aircraft",
    "type": "A320",
    "sn": "$SN",
    "regn": "$REGN",
    "entry": "$ENTRY",
    "exit": "$EXIT",
    "status": "Active"
  }
EOF
)

  FLEET_ARRAY="$FLEET_ARRAY$ITEM"
  
  if [ $i -lt 27 ]; then
    FLEET_ARRAY="$FLEET_ARRAY,"
  fi
done

FLEET_ARRAY="$FLEET_ARRAY]"
PAYLOAD="{\"fleetData\": $FLEET_ARRAY}"

# Write to a temporary file to avoid Bash escaping issues
echo "$PAYLOAD" > temp_fleet_payload.json

# ==========================================
# 🚀 SEND CURL REQUEST
# ==========================================
echo "Uploading 27 Aircraft to $API_URL..."

# Sending the token in 3 different headers to ensure the middleware catches it
curl -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -H "x-access-token: $TOKEN" \
  -H "token: $TOKEN" \
  -d @temp_fleet_payload.json

# Clean up the temp file
rm temp_fleet_payload.json

echo -e "\n\nDone! Check your database."