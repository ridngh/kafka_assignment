# Kafka Setup and Running Process for Windows Using Git Bash

## Steps Performed

1. **Installed Kafka and Java:**
   - Installed Kafka 4.0 from [Kafka Downloads](https://kafka.apache.org/downloads).
   - Installed Java 17 from [OpenJDK](https://openjdk.org/projects/jdk/17/).

2. **Unzipped Kafka:**
   - Unzipped Kafka into `C:/Downloads` to keep the folder close to the root directory, as longer file paths caused a max length reached error.

3. **Created Subfolder for Config:**
   - Inside the Kafka `config` folder, created a subfolder named `kraft` and copied over the `server.properties` file.

4. **Created Log Directory:**
   - Created a directory `/tmp/kraft-combined-logs/`.
   - Ran `chmod 777` to give the directory full access permissions.

5. **Modified Kafka Start Server File:**
   - Edited the Kafka start server file to resolve the `LOG4J_DIR` path by adding `file:////` in the path:
     ```bash
     LOG4J_DIR="file:///$base_dir/config/tools-log4j2.yaml"
     ```
   - This was necessary as the shell script implementation treated `C:` as a command instead of a directory, which required appending `file:///` for proper resolution.

6. **Generated `metadata.properties`:**
   - In `/tmp/kraft-combined-logs/`, generated `metadata.properties` by using a random UUID for a standalone server:
     ```bash
     $ bin/kafka-storage.sh format -t random -c config/kraft/server.properties --standalone
     ```
