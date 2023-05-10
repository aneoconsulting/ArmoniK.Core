locals {
  test-cmd = "exec test $$( mongosh --quiet --eval 'rs.status().ok' ) -eq 1"
}