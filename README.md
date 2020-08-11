# Enchilada

Enchilada: Environmental Chemistry through Intelligent Atmospheric Data Analysis.

# News

Enchilada has been through a major update for its 2.0 release. SQL Server has been removed. SQLite is used as the database instead. Installation is now dramatically easier, as the database is simply bundled in with the Enchilada install.


# How to install

Visit [our releases](https://github.com/dmusican/enchilada/releases), and choose the latest version.


# Support and updates

For any bugs that you find, please post a detailed [GitHub issue](https://github.com/dmusican/enchilada/issues).

Pull requests are welcome! Please include unit tests if you are adding new functionality.


# How to build from source

To build Enchilada, you will need Maven [installed on your machine](https://maven.apache.org/download.cgi): 

After installing Maven, you can clone this repo and use Maven from the command
line to build Enchilada. [Here are basic instructions for how to use
Maven.](https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html)
You can ignore the part about creating a new project; the POM for Enchilada is
included in the repo.

To get the project into IntelliJ, you can choose to import the repo directory as
an existing project. Choose to import it as a Maven project. All the default
options are fine, so just click through until the project is imported. You
should see the Maven toolbar pop up on the right side.
