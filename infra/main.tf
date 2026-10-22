terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0.0"
    }
  }
}

provider "docker" {
  host = "unix://${pathexpand("~")}/.orbstack/run/docker.sock"
}

resource "docker_image" "azurite" {
  name         = "mcr.microsoft.com/azure-storage/azurite:latest"
  keep_locally = true
}

resource "docker_container" "azurite_emulator" {
  image = docker_image.azurite.image_id
  name  = "bifrost-azurite-env"

  ports {
    internal = 10000
    external = 10000
  }
  ports {
    internal = 10001
    external = 10001
  }
  ports {
    internal = 10002
    external = 10002
  }
}

resource "null_resource" "create_containers" {
  depends_on = [docker_container.azurite_emulator]

  provisioner "local-exec" {
    command = <<EOT
      sleep 5 # Wait for the Azurite API to fully initialize
      export AZURITE_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
      az storage container create --name bronze --connection-string $AZURITE_CONN
      az storage container create --name silver --connection-string $AZURITE_CONN
      az storage container create --name gold --connection-string $AZURITE_CONN
    EOT
  }
}
