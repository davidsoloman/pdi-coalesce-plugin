# Coalesce Step - Help

The Coalesce Transformation step selects the first non null value from a group of input fields and passes it down the <br>
stream or returns null if all the fields are null. <br>
If Value Type option is specified, the output values will be converted to this data type. <br>
In case of type mismatches an error will be brought up during runtime. <br>
If Value Type is not specified the result will have the same data type as the input if all are equal or a more generic String data type otherwise.

## The Interface

A description of the options available in this step:

| Option                                 | Definition                                                         |
|----------------------------------------|--------------------------------------------------------------------|
| Step name                              | Name of this step as it appears in the transformation workspace    |
| OutputField                            | The name of the new field in which to store the step result        |
| Field A                                | First input field                                                  |
| Field B                                | Second input field                                                 |
| Field C                                | Third input field                                                  |
| Value Type                             | Data type for OutputField                                          |
| Remove                                 | Remove input fields from stream                                    |