.PHONY: all compile clean check test cover doc binopt certs compliance \
        h1-compliance h2-compliance h2c-compliance h3-compliance ws-compliance \
        h1spec-image h2spec-image h3spec-image autobahn-image help

# Tools
REBAR3 := rebar3
ERLC := erlc

# Compliance test images
# Tag carries the pinned upstream commit (short SHA); bump it together
# with the ARGs in priv/Dockerfile.h1spec so a new upstream pin rebuilds.
H1SPEC_VERSION := 0.2.0-f0a5650
H1SPEC_IMAGE := nhttp/h1spec:$(H1SPEC_VERSION)
H1SPEC_DOCKERFILE := priv/Dockerfile.h1spec
H2SPEC_IMAGE := summerwind/h2spec:2.6.0
H3SPEC_VERSION := 0.1.12
H3SPEC_IMAGE := nhttp/h3spec:$(H3SPEC_VERSION)
H3SPEC_DOCKERFILE := priv/Dockerfile.h3spec
AUTOBAHN_VERSION := 25.10.1
AUTOBAHN_IMAGE := nhttp/autobahn:$(AUTOBAHN_VERSION)
AUTOBAHN_DOCKERFILE := priv/Dockerfile.autobahn
AUTOBAHN_SPEC := priv/autobahn/fuzzingclient.json
AUTOBAHN_GATE := priv/autobahn/gate.sh
# Each suite starts its server through this script and exits with the
# status of the runner, so a failed suite fails the target.
COMPLIANCE_RUN := REBAR3=$(REBAR3) priv/compliance/run.sh
COMPLIANCE_REPORTS := compliance-reports
AUTOBAHN_REPORTS := $(COMPLIANCE_REPORTS)/autobahn
DOCKER_USER := --user $(shell id -u):$(shell id -g)
# h3spec only ships an x86_64 Linux binary; force amd64 so arm64 hosts
# (e.g. Apple Silicon, arm64 Linux with qemu-user) can still run it.
DOCKER_PLATFORM := linux/amd64

# Docker networking: on Linux, share the host stack so the container can
# reach 127.0.0.1:PORT directly. Elsewhere (macOS, Windows), the container
# has its own network namespace, so route to the host via the magic
# host.docker.internal name.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
    DOCKER_NET_ARGS := --network=host
    COMPLIANCE_TARGET := 127.0.0.1
else
    DOCKER_NET_ARGS := --add-host=host.docker.internal:host-gateway
    COMPLIANCE_TARGET := host.docker.internal
endif

# Test SSL certificates (auto-generated on demand)
TEST_CERTS_DIR := test/conf
TEST_CERTS_MARKER := $(TEST_CERTS_DIR)/server.pem
TEST_CERTS_SCRIPT := $(TEST_CERTS_DIR)/gen_test_certs.sh

#==============================================================================
# Core targets
#==============================================================================

all: compile

compile:
	$(REBAR3) compile

clean:
	$(REBAR3) clean
	@rm -rf $(COMPLIANCE_REPORTS)

check: certs
	$(REBAR3) check

test: certs
	$(REBAR3) test

cover: certs
	$(REBAR3) test
	$(REBAR3) cover --verbose
	@echo ""
	@echo "Quality gate: Coverage must be >= 85%"

#==============================================================================
# Test certificates
#==============================================================================

certs: $(TEST_CERTS_MARKER)

$(TEST_CERTS_MARKER): $(TEST_CERTS_SCRIPT)
	@echo "Generating test SSL certificates in $(TEST_CERTS_DIR)..."
	@cd $(TEST_CERTS_DIR) && ./gen_test_certs.sh >/dev/null

doc:
	$(REBAR3) ex_doc

#==============================================================================
# Binary optimization analysis
#==============================================================================

binopt:
	@for f in src/*.erl; do \
		$(ERLC) +bin_opt_info -o /tmp "$$f" 2>&1 | grep -E "^src/"; \
	done
	@rm -f /tmp/nhttp*.beam

#==============================================================================
# Compliance tests (Docker-based, no host install of h2spec/h3spec required)
#==============================================================================
h1-compliance: h1spec-image
	@echo "=== HTTP/1.1 Compliance Tests with h1spec (Docker) ==="
	@$(REBAR3) as test compile
	@$(COMPLIANCE_RUN) nhttp_h1_compliance \
		'docker run --rm $(DOCKER_NET_ARGS) $(H1SPEC_IMAGE) $(COMPLIANCE_TARGET) $$PORT'

h1spec-image:
	@command -v docker >/dev/null 2>&1 || { \
		echo "Error: docker not found in PATH. Install Docker to run compliance tests."; \
		exit 1; \
	}
	@docker image inspect $(H1SPEC_IMAGE) >/dev/null 2>&1 || { \
		echo "Building $(H1SPEC_IMAGE) from $(H1SPEC_DOCKERFILE)..."; \
		docker build -t $(H1SPEC_IMAGE) -f $(H1SPEC_DOCKERFILE) priv; \
	}

h2-compliance: certs h2spec-image
	@echo "=== HTTP/2 RFC 9113 Compliance Tests with h2spec (Docker) ==="
	@$(REBAR3) as test compile
	@mkdir -p $(COMPLIANCE_REPORTS)
	@$(COMPLIANCE_RUN) nhttp_compliance \
		'docker run --rm $(DOCKER_NET_ARGS) $(DOCKER_USER) \
			-v $(CURDIR)/$(COMPLIANCE_REPORTS):/reports $(H2SPEC_IMAGE) \
			-t -k -h $(COMPLIANCE_TARGET) -p $$PORT -o 5 -j /reports/h2spec.xml'

h2c-compliance: h2spec-image
	@echo "=== HTTP/2 cleartext (h2c, prior knowledge) Compliance Tests with h2spec (Docker) ==="
	@$(REBAR3) as test compile
	@mkdir -p $(COMPLIANCE_REPORTS)
	@$(COMPLIANCE_RUN) nhttp_compliance:start_h2c \
		'docker run --rm $(DOCKER_NET_ARGS) $(DOCKER_USER) \
			-v $(CURDIR)/$(COMPLIANCE_REPORTS):/reports $(H2SPEC_IMAGE) \
			-k -h $(COMPLIANCE_TARGET) -p $$PORT -o 5 -j /reports/h2spec-h2c.xml'

h2spec-image:
	@command -v docker >/dev/null 2>&1 || { \
		echo "Error: docker not found in PATH. Install Docker to run compliance tests."; \
		exit 1; \
	}
	@docker image inspect $(H2SPEC_IMAGE) >/dev/null 2>&1 || { \
		echo "Pulling $(H2SPEC_IMAGE)..."; \
		docker pull $(H2SPEC_IMAGE); \
	}

h3-compliance: certs h3spec-image
	@echo "=== HTTP/3 Compliance Tests with h3spec (Docker) ==="
	@$(REBAR3) as test compile
	@$(COMPLIANCE_RUN) nhttp_h3_compliance \
		'docker run --rm --platform $(DOCKER_PLATFORM) $(DOCKER_NET_ARGS) $(H3SPEC_IMAGE) \
			$(COMPLIANCE_TARGET) $$PORT -n -t 3000'

h3spec-image:
	@command -v docker >/dev/null 2>&1 || { \
		echo "Error: docker not found in PATH. Install Docker to run compliance tests."; \
		exit 1; \
	}
	@docker image inspect $(H3SPEC_IMAGE) >/dev/null 2>&1 || { \
		echo "Building $(H3SPEC_IMAGE) from $(H3SPEC_DOCKERFILE)..."; \
		docker build --platform $(DOCKER_PLATFORM) \
			-t $(H3SPEC_IMAGE) -f $(H3SPEC_DOCKERFILE) priv; \
	}

ws-compliance: autobahn-image
	@echo "=== WebSocket Compliance Tests with Autobahn (Docker) ==="
	@$(REBAR3) as test compile
	@rm -rf $(AUTOBAHN_REPORTS)
	@mkdir -p $(AUTOBAHN_REPORTS)
	@$(COMPLIANCE_RUN) nhttp_ws_compliance \
		'sed "s|ws://[^\"]*|ws://$(COMPLIANCE_TARGET):$$PORT/ws|" \
			$(AUTOBAHN_SPEC) > $(AUTOBAHN_REPORTS)/fuzzingclient.json \
		&& docker run --rm $(DOCKER_NET_ARGS) $(DOCKER_USER) \
			-v $(CURDIR)/$(AUTOBAHN_REPORTS):/reports $(AUTOBAHN_IMAGE) \
			wstest --mode fuzzingclient --spec /reports/fuzzingclient.json; \
		$(AUTOBAHN_GATE) $(AUTOBAHN_REPORTS)'

autobahn-image:
	@command -v docker >/dev/null 2>&1 || { \
		echo "Error: docker not found in PATH. Install Docker to run compliance tests."; \
		exit 1; \
	}
	@docker image inspect $(AUTOBAHN_IMAGE) >/dev/null 2>&1 || { \
		echo "Building $(AUTOBAHN_IMAGE) from $(AUTOBAHN_DOCKERFILE)..."; \
		docker build -t $(AUTOBAHN_IMAGE) -f $(AUTOBAHN_DOCKERFILE) priv; \
	}

compliance: h1-compliance h2-compliance h2c-compliance h3-compliance ws-compliance

#==============================================================================
# Help
#==============================================================================

help:
	@echo "nhttp Makefile targets:"
	@echo ""
	@echo "  Core:"
	@echo "    make              - Build the project"
	@echo "    make compile      - Build the project"
	@echo "    make clean        - Clean build artifacts"
	@echo "    make check        - Run fmt check, xref, dialyzer, hank"
	@echo "    make test         - Run all tests"
	@echo "    make cover        - Run tests with coverage (>= 85% required)"
	@echo "    make doc          - Generate ex_doc documentation"
	@echo ""
	@echo "  Quality gates (must pass after every phase):"
	@echo "    - All checks pass (make check)"
	@echo "    - Coverage >= 85% (make cover)"
	@echo ""
	@echo "  Analysis:"
	@echo "    make binopt       - Analyze binary optimization opportunities"
	@echo ""
	@echo "  Compliance (Docker-based, no host h2spec/h3spec install needed):"
	@echo "    make compliance      - Run all RFC compliance tests (h1spec + h2spec + h3spec + Autobahn)"
	@echo "    make h1-compliance   - Run HTTP/1.1 tests (Docker: $(H1SPEC_IMAGE))"
	@echo "    make h2-compliance   - Run HTTP/2 RFC 9113 tests (Docker: $(H2SPEC_IMAGE))"
	@echo "    make h2c-compliance  - Run HTTP/2 RFC 9113 tests in cleartext (Docker: $(H2SPEC_IMAGE))"
	@echo "    make h3-compliance   - Run HTTP/3 tests (Docker: $(H3SPEC_IMAGE))"
	@echo "    make ws-compliance   - Run WebSocket RFC 6455 tests (Docker: $(AUTOBAHN_IMAGE))"
	@echo ""
	@echo "  Each target exits with the status of its runner. Reports land in"
	@echo "  $(COMPLIANCE_REPORTS)/: h2spec.xml and h2spec-h2c.xml (JUnit), and autobahn/index.html."
	@echo "  make -k compliance runs every suite before it reports a failure."
	@echo ""
	@echo "  Requirement: Docker daemon running. On macOS/Windows, Docker Desktop."
	@echo ""
