# `distringo`

Distringo is (going to be) a redistricting application.

## Why?

Existing solutions for providing an interface for redistricting are either expensive or poorly-implemented.
Distringo aims to provide an option that makes no compromises on performance.

More to come later.

## Roadmap to v0.1.0

### Datasets

- [x] lib: Datasets can be parsed
- [x] lib: Datasets can be loaded
- [ ] main: Datasets can be queried via an "Index" action (returning an array of `id`s that can be queried)
- [ ] main+lib: Datasets can be queried via a "Show" action (returning the serialized representation of the data)
- [ ] main+lib: Logical records (rows of Datasets) can be queried via a "Show" action (returning the tables)
- [ ] main+lib: Dictionaries can be queried

### Shapefiles

- [ ] lib: Shapefiles can be parsed
- [ ] lib: Shapefiles can be loaded
  - [ ] Tabular Block (`tabblock`)
  - [ ] Block Group (`bg`)
- [ ] main+lib: Shapefiles can be queried via an "Index" action (returning an array of `id`s that can be queried)
- [ ] main+lib: Shapefiles can be queried via a "Show" action (returning the body of the shapefile)
- [ ] main+lib: Blocks can be queried from the in-memory `Shapefile` structures in `O(1)` time.
- [ ] main+lib: Tables can be retrieved from the in-memory `Shapefile` structures in `O(1)` time.

### Computations

- [ ] main+lib: Clients can request the "Convex Hull" of a JSON array of Polygon objects.
- [ ] main+lib: Clients can request the "Convex Hull" of a JSON array of Block identifiers.

### Sessions

- [ ] main: Sessions can be configured
- [ ] public: The client can change sessions

### Extra

- [ ] Every route under `/api/v0` is documented in its own documentation.
- [ ] An example instance is deployed (production and staging) under `distringo.rye.systems`.

## License

Distringo is released under the terms of the MIT License.
For the body of the license text, see the `LICENSE` file at the root of the source tree, or below.

```
Copyright (c) 2020 Kristofer Rye

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
