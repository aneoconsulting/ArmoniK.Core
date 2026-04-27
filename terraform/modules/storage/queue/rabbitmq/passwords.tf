resource "random_id" "rabbit_password_salt" {
  byte_length = 4
}

# RabbitMQ SHA256 hash: base64(salt + SHA256(salt + password))
data "external" "rabbit_guest_password_hash" {
  program = [
    "python3", "-c",
    "import hashlib,base64,json,sys;d=json.load(sys.stdin);s=bytes.fromhex(d['s']);p=d['p'].encode();print(json.dumps({'hash':base64.b64encode(s+hashlib.sha256(s+p).digest()).decode()}))"
  ]
  query = {
    s = random_id.rabbit_password_salt.hex
    p = "guest"
  }
}
