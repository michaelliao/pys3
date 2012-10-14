pys3
====

A simple Python interface for Amazon S3

Really simple:

    client = Client('access_key_id', 'access_key_secret', 'my.bucket.name')
    print client.put_object('path/hello.html', 'this is just a test and should return url')
    # http://my.bucket.name.s3.amazonaws.com/the/path/hello.html

    print client.get_object('the/path/hello.html')
    # file content as str...

    client.delete_object('the/path/hello.html')

    print client.get_object('the/path/hello.html')
    # Traceback (most recent call last):
    #   ...
    # StorageError: ('NoSuchKey', 'The specified key does not exist.')
