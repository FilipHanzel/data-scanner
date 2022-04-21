# Tool for data discovery

*Tested with python 3.8.5*

**Data scanner** is a tool for data/schema discovery able to iteratively go through a csv or json file (or files) and extract data types. It is capable of extracting one schema per file or one schema for all the files, if multiple files are provided.

Currently data scanner detects following types:
- *unknown*
- *integer*
- *float*
- *boolean*
- *date*
- *timestamp*
- *json*
- *string*

Data scanner will attempt to downcast as much as possible. For example if column has two values: `1` and `2.0`, an `integer` type will assigned to that column, since `2.0` can be safely converted to an integer.

Usage examples are available in examples folder.

### To be implemented:
- add `bit` type (only 0/1/nulls)
- test if using `is` operator would increase comparisson speeds in scanner classes
- add support for json lines format
- change negotiate flag in processor to be True as default
- allow passing a list of paths to the processor
- allow changing the list of values recognized as nulls
- allow changing the list of values recognized as booleans
- allow changing number of workers in multiprocessing run
- allow scanning csvs without a header
