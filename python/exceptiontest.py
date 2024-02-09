#!/usr/bin/python3.11

try:
  print("Here's variable x:", x)
except Exception as error:
  print("An error occurred:", error) # An error occurred: name 'x' is not defined
  print("An exception occurred:", type(error).__name__)

