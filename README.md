# Google Datastore ndb Client Library

[![Build Status](https://travis-ci.org/GoogleCloudPlatform/datastore-ndb-python.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/datastore-ndb-python)
[![Coverage Status](https://coveralls.io/repos/GoogleCloudPlatform/datastore-ndb-python/badge.svg?branch=master&service=github)](https://coveralls.io/github/GoogleCloudPlatform/datastore-ndb-python?branch=master)

## Introduction

This repository is for the original Datastore ndb client library.
If you are looking for Cloud NDB, which supports Python 3 and works both inside and outside of the Google App Engine environment, please see [this repository][0].

---
**Note:** As of Google App Engine SDK 1.6.4, ndb has reached status General Availability.  
    
Using ndb from outside of Google App Engine (without the use of Remote API) is currently a work in progress and has not been released.

---

ndb is a client library for use with [Google Cloud Datastore][1].
It was designed specifically to be used from within the 
[Google App Engine][2] Python runtime.

ndb is included in the Python runtime and is available through a
standard Python import.

    from google.appengine.ext import ndb

It is also possible to include ndb directly from this GitHub project.
This will allow application developers to manage their own dependencies. Note
however that ndb depends on the non-public Google Datastore App Engine RPC API. This means that there is no explicit support for older versions of ndb in the App Engine Python runtime.

## Overview

Learn how to use the ndb library by visiting the Google Cloud Platform 
[documentation][3].


[0]:https://github.com/googleapis/python-ndb
[1]:https://cloud.google.com/datastore
[2]:https://cloud.google.com/appengine
[3]:https://cloud.google.com/appengine/docs/python/ndb/
