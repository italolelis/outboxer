name: Main Workflow
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - name: Install Go
      uses: actions/setup-go@v1
      with:
        go-version: 1.13.x

    - name: Checkout code
      uses: actions/checkout@v1

    - name: Build the stack
      run: docker-compose -f build/docker-compose.yml up -d

    - name: Test
      run: make test

    - name: Down the stack
      run: docker-compose -f build/docker-compose.yml down -v

    - name: Upload coverage
      env:
        COVERALLS_TOKEN: ${{ secrets.COVERALLS_TOKEN }}
      run: |
        go get github.com/mattn/goveralls
        $GOPATH/bin/goveralls -coverprofile=tests.out -service travis-ci -repotoken $COVERALLS_TOKEN
