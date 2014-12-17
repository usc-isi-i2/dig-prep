import datetime
# print datetime.datetime.utcnow().isoformat()+'Z'
print datetime.datetime.utcnow().strftime("%Y-%m-%d--%H-%M-%S.%f")

