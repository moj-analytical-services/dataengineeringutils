# data Engineering Utils
A python package containing functions that help manage our data management processes on AWS

To install this package
`pip install git+git://github.com/moj-analytical-services/dataengineeringutils.git#egg=dataengineeringutils`

If you want to update the package then you need to delete it first before reinstalling i.e. run:
`pip uninstall dataengineeringutils`

**Warning:** This package has the following dependencies:
- numpy
- pandas
- io
- boto3

This package doesn't list its package denpencies because I found errors with io when installing via pip so I have left it blank for now ¯\\\_(ツ)\_/¯
