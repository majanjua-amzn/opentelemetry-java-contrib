val stableVersion = "1.39.0-adot1"
val alphaVersion = "1.39.0-alpha-adot1"

allprojects {
  if (findProperty("otel.stable") != "true") {
    version = alphaVersion
  } else {
    version = stableVersion
  }
}
