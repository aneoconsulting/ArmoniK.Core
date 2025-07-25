rs.initiate(
{
  _id: "${replica_set_name}",
  members: [
    {_id: 0, host: "127.0.0.1:27017"}
  ]
});

let initialized = false;
while(initialized === false)
{
  try {
    if (rs.status().myState === 1) {
      initialized = true;
      sleep(10000);
    }
  } catch (e) {
    sleep(1000);
  }
}
