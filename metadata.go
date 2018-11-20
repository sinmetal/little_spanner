package main

import (
	"os"

	"cloud.google.com/go/compute/metadata"
)

func GetProjectID() (string, error) {
	if !metadata.OnGCE() {
		return os.Getenv("GCLOUD_PROJECT"), nil
	}
	projectID, err := metadata.ProjectID()
	if err != nil {
		return "", err
	}
	return projectID, nil
}
