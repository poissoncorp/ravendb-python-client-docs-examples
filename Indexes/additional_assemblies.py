from ravendb import PutIndexesOperation, IndexDefinition, AdditionalAssembly

from examples_base import ExampleBase


class AdditionalAssemblies(ExampleBase):
    def setUp(self):
        super().setUp()

    def test_example(self):
        with self.embedded_server.get_document_store("AdditionalAssemblies") as store:
            with store.open_session() as session:
                # region complex_index
                store.maintenance.send(
                    PutIndexesOperation(
                        IndexDefinition(
                            name="Photographs/Tags",
                            maps={
                                """
                    from p in docs.Photographs
                    let photo = LoadAttachment(p, ""photo.png"")
                    where photo != null
                    let classified =  ImageClassifier.Classify(photo.GetContentAsStream())
                    select new {
                        e.Name,
                        Tag = classified.Where(x => x.Value > 0.75f).Select(x => x.Key),
                        _ = classified.Select(x => CreateField(x.Key, x.Value))
                    }
                    """
                            },
                            additional_sources={
                                "ImageClassifier": """
                    public static class ImageClassifier
                    {
                        public static IDictionary<string, float> Classify(Stream s)
                        {
                            // returns a list of descriptors with a
                            // value between 0 and 1 of how well the
                            // image matches that descriptor.
                        }
                    }
                    """
                            },
                            additional_assemblies={
                                AdditionalAssembly.from_runtime("System.Memory"),
                                AdditionalAssembly.from_NuGet("System.Drawing.Common", "4.7.0"),
                                AdditionalAssembly.from_NuGet("Microsoft.ML", "1.5.2"),
                            },
                        )
                    )
                )
                # endregion
                # region simple_indexes
                runtime_index = IndexDefinition(
                    name="Dog_Pictures",
                    maps={
                        """
                        from user in docs.Users
                        let fileName = Path.GetFileName(user.ImagePath)
                        where fileName = ""My_Dogs.jpeg""
                        select new {
                            user.Name,
                            fileName
                        }
                        """
                    },
                    additional_assemblies={AdditionalAssembly.from_runtime("System.IO")},
                )
                # endregion

                runtime_index = IndexDefinition(
                    name="Dog_Pictures",
                    maps={
                        """
                        from user in docs.Users
                        let fileName = Path.GetFileName(user.ImagePath)
                        where fileName = ""My_Dogs.jpeg""
                        select new {
                            user.Name,
                            fileName
                        }
                        """
                    },
                )
                # region prerelease_assembly
                additional_assemblies = {
                    AdditionalAssembly.from_NuGet("FlexiMvvm.Common.PreRelease", "0.18.8-prerelease")
                }
                # endregion
