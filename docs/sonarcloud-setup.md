# SonarCloud Integration Setup

Documents the one-time SonarCloud console change and all code changes required to enable CI-based analysis with coverage reporting for this project.

---

## 1. SonarCloud Console: Disable Automatic Analysis

SonarCloud's **Automatic Analysis** and **CI-based analysis** are mutually exclusive. If both are active the CI build fails with:

```
You are running CI analysis while Automatic Analysis is enabled.
```

**Steps to disable:**

1. Log in to [sonarcloud.io](https://sonarcloud.io)
2. Open the **mango4j-swarm** project (not the organisation)
3. **Administration → Analysis Method**
4. Toggle **Automatic Analysis** off

This is a one-time change per project.

---

## 2. GitHub Actions Secrets Required

| Secret | Value | Scope |
|---|---|---|
| `SONAR_TOKEN` | Generated per repository — see below | Per-repo |
| `SONAR_HOST_URL` | `https://sonarcloud.io` | Shared (same for every repo) |
| `GPG_PRIVATE_KEY` | Signing key for Maven Central publishing | Release/snapshot workflows only |
| `GPG_PASSPHRASE` | Passphrase for the GPG key | Release/snapshot workflows only |
| `CENTRAL_USERNAME` | Maven Central portal username | Release/snapshot workflows only |
| `CENTRAL_PASSWORD` | Maven Central portal password | Release/snapshot workflows only |
| `APP_ID` | GitHub App ID for bot commits | Release/snapshot workflows only |
| `APP_PRIVATE_KEY` | GitHub App private key for bot commits | Release/snapshot workflows only |

### Generating `SONAR_TOKEN`

A token must be generated separately for each repository.

1. Log in to [sonarcloud.io](https://sonarcloud.io)
2. Go to **My Account → Security**
3. Under **Generate Tokens**, enter a name (e.g. `mango4j-swarm-ci`) and click **Generate**
4. Copy the token — it is only shown once
5. In the GitHub repository go to **Settings → Secrets and variables → Actions**
6. Add a new repository secret named `SONAR_TOKEN` with the copied value

### Setting `SONAR_HOST_URL`

This value is the same for every project using SonarCloud and never changes:

```
https://sonarcloud.io
```

Add it as a repository secret named `SONAR_HOST_URL` (or as an organisation-level secret so it is shared across all repositories automatically).

---

## 3. pom.xml Changes

### 3.1 Sonar Project Properties

Added to `<properties>` so the scanner identifies the correct SonarCloud project without requiring CLI flags:

```xml
<sonar.projectKey>bitstep-ie_mango4j-swarm</sonar.projectKey>
<sonar.organization>bitstep-ie</sonar.organization>
<sonar.coverage.jacoco.xmlReportPaths>target/site/jacoco/jacoco.xml</sonar.coverage.jacoco.xmlReportPaths>
```

**Why `target/site/jacoco/jacoco.xml` and not `tmp/`?**

The project overrides Maven's build directory to `tmp/` (`<build><directory>${project.basedir}/tmp</directory>`). SonarScanner auto-detects coverage only at the standard `target/site/jacoco/jacoco.xml`. Using a Maven property expression like `${project.build.directory}` in `<properties>` is unreliable because property interpolation ordering between `<properties>` and `<build><directory>` is not guaranteed. The JaCoCo report goal is therefore configured explicitly to write to `target/site/jacoco/` regardless of the build directory, and `sonar.coverage.jacoco.xmlReportPaths` points to the same fixed relative path.

### 3.2 Default GPG Skip

```xml
<gpg.skip>true</gpg.skip>
```

GPG signing is a publish-only concern. Defaulting to `true` means local builds (`mvn verify`, `hammer-time` profile) and CI analysis runs never fail due to a missing GPG key. The two publish workflows (`release.yml`, `snapshots.yml`) override this with `-Dgpg.skip=false` at their deploy step.

### 3.3 JaCoCo `coverage` Profile

Following [SonarCloud's documented approach](https://docs.sonarsource.com/sonarqube-cloud/advanced-setup/languages/java/), JaCoCo lives in a dedicated `coverage` profile rather than the main build. This keeps local `mvn verify` and `hammer-time` runs free of instrumentation overhead.

```xml
<profile>
    <id>coverage</id>
    <build>
        <plugins>
            <plugin>
                <groupId>org.jacoco</groupId>
                <artifactId>jacoco-maven-plugin</artifactId>
                <version>${jacoco-maven-plugin.version}</version>
                <executions>
                    <execution>
                        <id>prepare-agent</id>
                        <goals><goal>prepare-agent</goal></goals>
                    </execution>
                    <execution>
                        <id>report</id>
                        <goals><goal>report</goal></goals>
                        <configuration>
                            <!-- dataFile defaults to ${project.build.directory}/jacoco.exec = tmp/jacoco.exec -->
                            <dataFile>${project.build.directory}/jacoco.exec</dataFile>
                            <!-- outputDirectory pinned to target/ so SonarScanner auto-detects it -->
                            <outputDirectory>${project.basedir}/target/site/jacoco</outputDirectory>
                            <formats><format>XML</format></formats>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</profile>
```

**Key detail:** `dataFile` stays at `${project.build.directory}/jacoco.exec` (`tmp/jacoco.exec`) because that is where the JaCoCo agent writes during test execution. `outputDirectory` is pinned to `${project.basedir}/target/site/jacoco` using `project.basedir` (always absolute and correct) to avoid any dependency on `project.build.directory` resolution.

---

## 4. CI Workflow Changes (`packages.yml`)

### 4.1 GPG and Central Credentials Removed from CI

GPG signing and Maven Central credentials are not needed for building or scanning. Removing them from `setup-java` and step envs prevents GPG agent issues from interfering with the CI lifecycle.

Before:
```yaml
- uses: actions/setup-java@v4
  with:
    server-id: central
    server-username: CENTRAL_USERNAME
    server-password: CENTRAL_PASSWORD
    gpg-private-key: ${{ secrets.GPG_PRIVATE_KEY }}
    gpg-passphrase: GPG_PASSPHRASE
```

After:
```yaml
- uses: actions/setup-java@v4
  with:
    distribution: temurin
    java-version: 17
    cache: maven
```

### 4.2 Build Step Activates the `coverage` Profile

`-Pcoverage` is added to the Build step so JaCoCo runs during `mvn verify`, executes tests with the agent, and writes `target/site/jacoco/jacoco.xml` before the Sonar step reads it.

```yaml
- name: Build with Maven
  run: mvn --batch-mode --no-transfer-progress verify -Pcoverage -Dgpg.skip=true
```

### 4.3 Sonar Step Reads the Pre-generated Report

`sonar-maven-plugin` 5.x **does not trigger the Maven lifecycle** — `sonar:sonar` runs standalone. The Sonar step therefore only sends analysis to SonarCloud; it relies on the XML report already written by the Build step.

```yaml
- name: SonarQube Scan
  if: github.event_name != 'pull_request'
  env:
    SONAR_TOKEN: ${{ secrets.SONAR_TOKEN }}
    SONAR_HOST_URL: ${{ secrets.SONAR_HOST_URL }}
  run: |
    mvn --batch-mode --no-transfer-progress \
      org.sonarsource.scanner.maven:sonar-maven-plugin:sonar \
      -Pcoverage \
      -Dgpg.skip=true \
      -Dsonar.token=${SONAR_TOKEN} \
      -Dsonar.host.url=${SONAR_HOST_URL} \
      -Dsonar.branch.name=${GITHUB_REF_NAME}
```

PR analysis uses `-Dsonar.pullrequest.*` parameters instead of `-Dsonar.branch.name`.

---

## 5. End-to-End Flow

```
push to main
    │
    ▼
[Build with Maven]
mvn verify -Pcoverage -Dgpg.skip=true
    │
    ├─ compile
    ├─ jacoco:prepare-agent  → instruments JVM (argLine)
    ├─ test                  → runs tests, writes tmp/jacoco.exec
    ├─ jacoco:report         → reads tmp/jacoco.exec
    │                          writes target/site/jacoco/jacoco.xml
    └─ verify passes ✓
    │
    ▼
[SonarQube Scan]
mvn sonar:sonar -Pcoverage -Dgpg.skip=true -Dsonar.token=...
    │
    └─ sonar:sonar (standalone, no lifecycle)
           reads pom properties (sonar.projectKey, sonar.organization,
                                  sonar.coverage.jacoco.xmlReportPaths)
           reads target/site/jacoco/jacoco.xml
           sends analysis + coverage to SonarCloud ✓
```
