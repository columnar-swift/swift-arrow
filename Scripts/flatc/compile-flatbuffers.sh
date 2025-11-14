#!/bin/bash
container run -v "$(pwd)":/src flatc File.fbs
