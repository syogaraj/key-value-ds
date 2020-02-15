# key-value-ds
File based Key-Value datastore

Supports basic CRD (Create, Read, Delete)

Functionalities:
  1. It can be initialized using an optional file path. If one is not provided, it will reliably
create itself.
  2. Key string capped at 32 characters and Value must be a JSON object capped at 16KB. (These values can be changed in config.py)
  3. Every key supports setting a Time-To-Live property when it is created. This property is optional. If provided, it will be evaluated as an integer defining the number of seconds. Once the Time-To-Live for a key has expired,
the key will no longer be available for Read or Delete operations.
  4. Only one process can access the datastore (local file) at a time.
  5. Thread safe.
