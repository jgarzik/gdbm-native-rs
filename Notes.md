
# Notes about this library

## Implementation notes

### Excessive copying

Modifications to the bucket data structure look something like

* `let bucket = clone-of-bucket-in-HashMap`
* Use bucket
* Insert bucket in HashMap, overwriting old version

It would be better to reference the data structure and mutate it directly.

