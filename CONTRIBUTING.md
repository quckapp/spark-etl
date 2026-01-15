# Contributing to QuckApp

Thank you for your interest in contributing to QuckApp! This document provides guidelines and instructions for contributing.

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for everyone.

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Node.js 20+
- Go 1.21+
- Python 3.12+
- Java 17+
- Elixir 1.15+

### Development Setup

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/quckapp/quckapp.git
cd quckapp

# Start infrastructure
cd infrastructure/docker
cp .env.example .env
docker compose -f docker-compose.infra.yml up -d

# Install dependencies for the service you're working on
cd services/<service-name>
# Follow service-specific setup instructions
```

## How to Contribute

### Reporting Bugs

1. Check existing issues to avoid duplicates
2. Use the bug report template
3. Include:
   - Clear description of the issue
   - Steps to reproduce
   - Expected vs actual behavior
   - Environment details (OS, versions)
   - Relevant logs or screenshots

### Suggesting Features

1. Check existing feature requests
2. Use the feature request template
3. Describe the use case and benefits
4. Consider implementation approach

### Submitting Changes

#### Branch Naming

```
feature/<description>    # New features
fix/<description>        # Bug fixes
docs/<description>       # Documentation
refactor/<description>   # Code refactoring
test/<description>       # Test additions/changes
```

#### Commit Messages

Follow conventional commits:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

Examples:
```
feat(auth): add OAuth2 Google provider
fix(message-service): resolve race condition in broadcast
docs(api): update WebSocket endpoint documentation
```

#### Pull Request Process

1. **Fork & Branch**: Create a feature branch from `main`
2. **Develop**: Make your changes following code standards
3. **Test**: Ensure all tests pass
4. **Document**: Update relevant documentation
5. **PR**: Submit a pull request with:
   - Clear title and description
   - Link to related issues
   - Screenshots for UI changes
   - Test evidence

### Code Standards

#### General

- Write clean, readable, self-documenting code
- Follow existing patterns in the codebase
- Add comments only for complex logic
- Keep functions focused and small

#### TypeScript/JavaScript (NestJS)

```bash
npm run lint
npm run format
npm run test
```

#### Java (Spring Boot)

```bash
./mvnw spotless:check
./mvnw test
```

#### Go

```bash
go fmt ./...
go vet ./...
go test ./...
```

#### Python (FastAPI)

```bash
ruff check .
black --check .
pytest
```

#### Elixir (Phoenix)

```bash
mix format --check-formatted
mix credo
mix test
```

### Testing Requirements

- Unit tests for new functionality
- Integration tests for API endpoints
- Maintain or improve code coverage
- All CI checks must pass

## Project Structure

```
QuckApp/
├── admin/           # Admin dashboard (React)
├── docs/            # Documentation (Docusaurus)
├── infrastructure/  # DevOps & IaC
├── mobile/          # Mobile app (React Native)
├── packages/        # Shared packages
├── services/        # Microservices (33 total)
└── web/             # Web app (React)
```

### Service Categories

| Stack | Services |
|-------|----------|
| NestJS | backend-gateway, notification-service, realtime-service |
| Spring Boot | auth, user, permission, audit, admin |
| Go | workspace, channel, thread, search, file, media, etc. |
| Elixir | presence, message, call, huddle, event-broadcast, etc. |
| Python | analytics, ml, moderation, sentiment, insights, etc. |

## Communication

- **Issues**: For bugs and feature requests
- **Discussions**: For questions and ideas
- **Pull Requests**: For code contributions

## Recognition

Contributors will be recognized in:
- Release notes
- Contributors list
- Project documentation

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing to QuckApp!
