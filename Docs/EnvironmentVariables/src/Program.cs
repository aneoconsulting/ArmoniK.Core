using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Xml.Linq;

namespace ArmoniK.Core.Docs.EnvironmentVariables;

internal static class Program
{
  private static void Main()
  {
    var solutionDirectory = GetSolutionRootDirectory();

    var                                     processedFiles    = new HashSet<string>();
    var xmlFiles = Directory.GetFiles(solutionDirectory,
                                      "*.xml",
                                      SearchOption.AllDirectories);

    var pdfSections       = (
                              from file in xmlFiles
                              let fileHash = ComputeFileHash(file)
                              where processedFiles.Add(fileHash)
                              let properties = ExtractPropertiesFromXml(file)
                              where properties.Count != 0
                              select (Path.GetFileNameWithoutExtension(file), properties)).ToList();

    CreateMarkdown(pdfSections);
  }

  private static string ComputeFileHash(string filePath)
  {
    try
    {
      using var sha256 = SHA256.Create();
      using var stream = File.OpenRead(filePath);
      var       hash   = sha256.ComputeHash(stream);
      return Convert.ToBase64String(hash);
    }
    catch (Exception e)
    {
      Console.WriteLine($"An error occurred while processing file: {filePath}. Error: {e.Message}");
      throw;
    }
  }

  private static string GetSolutionRootDirectory()
  {
    var currentDirectory = Directory.GetCurrentDirectory();
    while (!File.Exists(Path.Combine(currentDirectory,
                                     $"{Path.GetFileName(currentDirectory)}.sln")))
    {
      currentDirectory = Directory.GetParent(currentDirectory)
                                  ?.FullName!;
      if (currentDirectory == null)
      {
        throw new Exception("Solution root directory not found.");
      }
    }

    return currentDirectory;
  }

  private static List<(string VarName, string Description)> ExtractPropertiesFromXml(string xmlFilePath)
  {
    var properties = new List<(string VarName, string Description)>();
    var doc        = XDocument.Load(xmlFilePath);

    const string sectionName = "SettingSection";
    const string prefix      = "F:";

    // Find all classes with a SettingsSection
    var settingsSections = doc.Descendants("member")
                              .Where(m => m.Attribute("name")
                                           ?.Value.Contains(sectionName) == true)
                              .Select(m =>
                                      {
                                        var fullName = m.Attribute("name")
                                                        ?.Value;
                                        if (fullName != null)
                                        {
                                          // Find the index of the prefix and the section name
                                          var startIndex = fullName.IndexOf(prefix,
                                                                            StringComparison.Ordinal) + prefix.Length;
                                          var endIndex = fullName.LastIndexOf(sectionName,
                                                                              StringComparison.Ordinal);

                                          // Extract the substring if both indices are valid
                                          if (startIndex > prefix.Length - 1 && endIndex > startIndex)
                                          {
                                            return fullName.Substring(startIndex,
                                                                      endIndex - startIndex - 1); // -1 to remove trailing dot
                                          }
                                        }

                                        return null; // Return null if conditions are not met
                                      });
    foreach (var className in settingsSections)
    {
      Console.WriteLine(className);
      var pp = doc.Descendants("member")
                  .Where(m => m.Attribute("name")
                               ?.Value.StartsWith("P:") == true);


      var propertyMembers = doc.Descendants("member")
                               .Where(m => m.Attribute("name")
                                            ?.Value.StartsWith($"P:{className}") == true);

      foreach (var member in propertyMembers)
      {
        var name = member.Attribute("name")
                         ?.Value;
        var description = member.Element("summary")
                                ?.Value.Trim() ?? "No description available.";

        if (name == null)
        {
          continue; // Skip if name is null
        }

        var relevantPropertyName = name[2..]; // Remove "P:"

        // Split the property name by '.' and get the part after the second-to-last dot
        var nameParts = relevantPropertyName.Split('.');
        if (nameParts.Length < 2)
        {
          continue;
        }

        // Get the part after the second-to-last dot
        var dashedPropertyName = nameParts[^2] + "__" + nameParts.Last();

        properties.Add((dashedPropertyName, description));
      }
    }
    return properties;
  }

  private static void CreateMarkdown(List<(string SectionName, List<(string VarName, string Description)> EnvVars)> sections)
  {
    var markdownContent = "# ArmoniK Environment Variables\n\n";

    foreach (var section in sections.OrderBy(section => section.SectionName))
    {
      markdownContent += $"## {section.SectionName}\n\n";
      foreach (var env in section.EnvVars)
      {
        markdownContent += $"- **{env.VarName}**: {env.Description}\n";
      }

      markdownContent += "\n";
    }

    File.WriteAllText("ArmoniKEnvironmentVariables.md",
                      markdownContent);
    Console.WriteLine("Markdown file generated successfully.");
  }
}
