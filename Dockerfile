#build stage
FROM golang:latest

# Set our working directory into which we will copy our app, inside the container
WORKDIR /work

# copy go.mod and go.sum to the working directory inside the container
COPY go.mod go.sum ./
COPY . .

# download dependencies, dependencies will be cached if the go.mod and go.sum 
RUN go mod download

# Copy the source from the current directory to the working directory inside the container  
COPY . .

# Build the go app, output the work directory into the cmd sub-directory
RUN go build -o ./cmd/consumer ./cmd

# Execute/run the program
CMD ./consumer
