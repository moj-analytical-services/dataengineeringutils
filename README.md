# data_engineering_utils
A python package containing functions that help manage our data management processes on AWS

To install this package
`pip install git+git://github.com/moj-analytical-services/data_engineering_utils.git#egg=data_engineering_utils`

If you want to update the package then you need to delete it first before reinstalling i.e. run:
`pip uninstall data_engineering_utils`

**Warning:** This package has the following dependencies:
- numpy
- pandas
- io
- boto3

This package doesn't list its package denpencies because I found errors with io when installing via pip so I have left it blank for now ¯\\\_(ツ)\_/¯
