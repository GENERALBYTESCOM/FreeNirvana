using System;
using System.Collections.Generic;
using System.IO;
using System.Xml;
using System.Xml.Linq;
using IO;

namespace SAUtils.InputFileParsers.ClinVar
{
    public sealed class ClinVarVariationReader : IDisposable
    {
        private const string VcvRecordTag      = "VariationArchive";
        private const string AccessionTag      = "Accession";
        private const string VersionTag        = "Version";
        private const string DateTag           = "DateLastUpdated";
        private const string ReviewStatusTag   = "ReviewStatus";
        private const string InterpretedRecordTag = "InterpretedRecord";
        private const string InterpretationsTag   = "Interpretations";
        private const string InterpretationTag    = "Interpretation";

        private const string IncludedRecordTag = "IncludedRecord";
        private const string ClassifiedRecordTag = "ClassifiedRecord";
        private const string ClassificationsTag = "Classifications";
        private const string GermlineClassificationTag = "GermlineClassification";
        private const string OncogenicityClassificationTag = "OncogenicityClassification";
        private const string SomaticClinicalImpactTag = "SomaticClinicalImpact";
        private const string NoClassificationTag = "NoClassification";
        
        private const string DescriptionTag = "Description";
        private const string ExplanationTag = "Explanation";
        private const string TypeTag        = "Type";


        private readonly Stream _readStream;

        public ClinVarVariationReader(Stream readStream)
        {
            _readStream = readStream;
        }

        public IEnumerable<VcvItem> GetItems()
        {
            using (var reader = FileUtilities.GetStreamReader(_readStream))
            using (var xmlReader = XmlReader.Create(reader, new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit, IgnoreWhitespace = true}))
            {
                xmlReader.ReadToDescendant(VcvRecordTag);
                do
                {
                    var  subTreeReader = xmlReader.ReadSubtree();
                    var xElement       = XElement.Load(subTreeReader);
                    
                    var item = ExtractVariantRecord(xElement);
                    
                    if (item == null) continue;
                    yield return item;

                } while (xmlReader.ReadToNextSibling(VcvRecordTag));
            }
        }

        private static VcvItem ExtractVariantRecord(XElement xElement)
        {
            if (xElement == null || xElement.IsEmpty) return null;
            
            var accession  = xElement.Attribute(AccessionTag)?.Value;
            var version    = xElement.Attribute(VersionTag)?.Value;
            var dateString       = xElement.Attribute(DateTag)?.Value;
            var date        = ClinVarParser.ParseDate(dateString);

            var classifiedRecord = xElement.Element(ClassifiedRecordTag);
            var interpretationRecord = xElement.Element(InterpretedRecordTag);
            var includedRecord = xElement.Element(IncludedRecordTag);
            
            // Count non-null, non-empty records
            int recordCount = 0;
            if (classifiedRecord != null && !classifiedRecord.IsEmpty) recordCount++;
            if (interpretationRecord != null && !interpretationRecord.IsEmpty) recordCount++;
            if (includedRecord != null && !includedRecord.IsEmpty) recordCount++;
            
            if (recordCount != 1)
            {
                throw new DataMisalignedException($"Exactly one of ClassifiedRecord/InterpretedRecord/IncludedRecord should be present for {accession}");
            }
            
            // Handle ClassifiedRecord (ClinVar VCV 2.5+ format - most common)
            if (classifiedRecord != null && !classifiedRecord.IsEmpty)
            {
                return ExtractClassifiedRecord(classifiedRecord, accession, version, date);
            }
            
            // Handle InterpretedRecord (older format)
            if (interpretationRecord != null && !interpretationRecord.IsEmpty)
            {
                var interpretedSignificances = GetSignificances(interpretationRecord.Element(InterpretationsTag));

                var interpretedReviewStatusString = interpretationRecord.Element(ReviewStatusTag)?.Value;
                if(interpretedReviewStatusString ==null) throw new MissingFieldException($"No review status provided for {accession}.{version}");
            
                var interpretedReviewStatus = ClinVarCommon.ReviewStatusNameMapping[interpretedReviewStatusString];
                return new VcvItem(accession, version, date, interpretedReviewStatus, interpretedSignificances);
            }
            
            // Handle IncludedRecord (legacy format)
            // ClinVar VCV 2.5: Some IncludedRecords now have Classifications element (new format)
            var includedClassifications = includedRecord.Element(ClassificationsTag);
            if (includedClassifications != null && !includedClassifications.IsEmpty)
            {
                // New VCV 2.5 format with Classifications element - parse like ClassifiedRecord
                return ExtractClassifiedRecord(includedRecord, accession, version, date);
            }
            
            // Old format: direct ReviewStatus element
            var includedSignificances = GetSignificances(includedRecord.Element(InterpretationsTag));

            var includedReviewStatusString = includedRecord.Element(ReviewStatusTag)?.Value;
            if(includedReviewStatusString ==null) throw new MissingFieldException($"No review status provided for {accession}.{version}");
            
            var reviewStatus = ClinVarCommon.ReviewStatusNameMapping[includedReviewStatusString];
            return new VcvItem(accession, version, date, reviewStatus, includedSignificances);
        }

        private static VcvItem ExtractClassifiedRecord(XElement classifiedRecord, string accession, string version, long date)
        {
            var classifications = classifiedRecord.Element(ClassificationsTag);
            if (classifications == null || classifications.IsEmpty)
            {
                throw new MissingFieldException($"No Classifications element found for {accession}.{version}");
            }

            // Check for all classification types
            var germlineClassification = classifications.Element(GermlineClassificationTag);
            var oncogenicityClassification = classifications.Element(OncogenicityClassificationTag);
            var somaticClinicalImpact = classifications.Element(SomaticClinicalImpactTag);
            var noClassification = classifications.Element(NoClassificationTag);

            // Count how many non-empty classification types we have
            var classificationTypes = new List<(XElement element, string type)>();
            if (germlineClassification != null && !germlineClassification.IsEmpty)
                classificationTypes.Add((germlineClassification, "germline"));
            if (oncogenicityClassification != null && !oncogenicityClassification.IsEmpty)
                classificationTypes.Add((oncogenicityClassification, "oncogenicity"));
            if (somaticClinicalImpact != null && !somaticClinicalImpact.IsEmpty)
                classificationTypes.Add((somaticClinicalImpact, "somatic"));

            // Handle multiple classifications (2 or 3 types present)
            if (classificationTypes.Count >= 2)
            {
                return ExtractMultipleClassifications(classificationTypes, accession, version, date);
            }

            // Handle single classification types
            if (germlineClassification != null && !germlineClassification.IsEmpty)
            {
                return ExtractGermlineClassification(germlineClassification, accession, version, date);
            }

            if (oncogenicityClassification != null && !oncogenicityClassification.IsEmpty)
            {
                return ExtractOncogenicityClassification(oncogenicityClassification, accession, version, date);
            }

            if (somaticClinicalImpact != null && !somaticClinicalImpact.IsEmpty)
            {
                return ExtractSomaticClinicalImpact(somaticClinicalImpact, accession, version, date);
            }

            // Check for NoClassification (evidence-only records)
            if (noClassification != null && !noClassification.IsEmpty)
            {
                return ExtractNoClassification(noClassification, accession, version, date);
            }

            // If we get here, there's a Classifications element but no recognized classification type
            throw new MissingFieldException($"No recognized classification type found for {accession}.{version}");
        }

        private static VcvItem ExtractGermlineClassification(XElement germlineClassification, string accession, string version, long date)
        {
            var reviewStatusString = germlineClassification.Element(ReviewStatusTag)?.Value;
            if (reviewStatusString == null)
            {
                throw new MissingFieldException($"No review status provided for {accession}.{version}");
            }

            var reviewStatus = ClinVarCommon.ReviewStatusNameMapping[reviewStatusString];

            // Extract significance from Description element
            var descriptionElement = germlineClassification.Element(DescriptionTag);
            var significances = GetClassifiedRecordSignificances(descriptionElement);

            return new VcvItem(accession, version, date, reviewStatus, significances);
        }

        private static VcvItem ExtractOncogenicityClassification(XElement oncogenicityClassification, string accession, string version, long date)
        {
            var reviewStatusString = oncogenicityClassification.Element(ReviewStatusTag)?.Value;
            if (reviewStatusString == null)
            {
                throw new MissingFieldException($"No review status provided for {accession}.{version}");
            }

            var reviewStatus = ClinVarCommon.ReviewStatusNameMapping[reviewStatusString];

            // Extract significance from Description element
            var descriptionElement = oncogenicityClassification.Element(DescriptionTag);
            var significances = GetClassifiedRecordSignificances(descriptionElement);

            return new VcvItem(accession, version, date, reviewStatus, significances);
        }

        private static VcvItem ExtractMultipleClassifications(List<(XElement element, string type)> classificationTypes, string accession, string version, long date)
        {
            var combinedSignificances = new List<string>();
            var reviewStatuses = new List<ClinVarCommon.ReviewStatus>();

            // Extract review status and significances from all classification types
            foreach (var (element, type) in classificationTypes)
            {
                var reviewStatusString = element.Element(ReviewStatusTag)?.Value;
                if (reviewStatusString == null)
                {
                    throw new MissingFieldException($"No review status provided for {type} classification in {accession}.{version}");
                }
                var reviewStatus = ClinVarCommon.ReviewStatusNameMapping[reviewStatusString];
                reviewStatuses.Add(reviewStatus);

                var descriptionElement = element.Element(DescriptionTag);
                var significances = GetClassifiedRecordSignificances(descriptionElement);
                if (significances != null)
                    combinedSignificances.AddRange(significances);
            }

            // Use the highest-confidence review status (prefer expert_panel > practice_guideline > etc.)
            var finalReviewStatus = GetHighestReviewStatus(reviewStatuses);

            return new VcvItem(accession, version, date, finalReviewStatus, combinedSignificances.Count > 0 ? combinedSignificances : null);
        }

        private static ClinVarCommon.ReviewStatus GetHigherReviewStatus(ClinVarCommon.ReviewStatus status1, ClinVarCommon.ReviewStatus status2)
        {
            // Return the status with higher confidence level
            // practice_guideline (7) > expert_panel (6) > multiple_submitters_no_conflict (5) > multiple_submitters (4) > conflicting_interpretations (3) > single_submitter (2) > no_criteria (1) > no_assertion (0)
            return status1 > status2 ? status1 : status2;
        }

        private static ClinVarCommon.ReviewStatus GetHighestReviewStatus(List<ClinVarCommon.ReviewStatus> statuses)
        {
            // Return the highest confidence level from multiple review statuses
            var highest = ClinVarCommon.ReviewStatus.no_assertion;
            foreach (var status in statuses)
            {
                if (status > highest)
                    highest = status;
            }
            return highest;
        }

        private static VcvItem ExtractNoClassification(XElement noClassification, string accession, string version, long date)
        {
            var reviewStatusString = noClassification.Element(ReviewStatusTag)?.Value;
            if (reviewStatusString == null)
            {
                throw new MissingFieldException($"No review status provided for {accession}.{version}");
            }

            var reviewStatus = ClinVarCommon.ReviewStatusNameMapping[reviewStatusString];

            // Extract significance from Description element
            var descriptionElement = noClassification.Element(DescriptionTag);
            var significances = GetClassifiedRecordSignificances(descriptionElement);

            return new VcvItem(accession, version, date, reviewStatus, significances);
        }

        private static VcvItem ExtractSomaticClinicalImpact(XElement somaticClinicalImpact, string accession, string version, long date)
        {
            var reviewStatusString = somaticClinicalImpact.Element(ReviewStatusTag)?.Value;
            if (reviewStatusString == null)
            {
                throw new MissingFieldException($"No review status provided for {accession}.{version}");
            }

            var reviewStatus = ClinVarCommon.ReviewStatusNameMapping[reviewStatusString];

            // Extract significance from Description element
            var descriptionElement = somaticClinicalImpact.Element(DescriptionTag);
            var significances = GetClassifiedRecordSignificances(descriptionElement);

            return new VcvItem(accession, version, date, reviewStatus, significances);
        }

        private static List<string> GetClassifiedRecordSignificances(XElement descriptionElement)
        {
            if (descriptionElement == null || string.IsNullOrEmpty(descriptionElement.Value))
            {
                return null;
            }

            var description = descriptionElement.Value.ToLower();
            var significances = ClinVarCommon.GetSignificances(description, null);
            var significanceList = new List<string>();

            foreach (var significance in significances)
            {
                if (!ClinVarCommon.ValidPathogenicity.Contains(significance))
                {
                    throw new InvalidDataException($"Invalid clinical significance found. Observed: {significance}");
                }
                significanceList.Add(significance);
            }

            return significanceList.Count > 0 ? significanceList : null;
        }
        
        private static List<string> GetSignificances(XElement interpretations)
        {
            if (interpretations == null || interpretations.IsEmpty) return null;
            
            var significanceList = new List<string>();
            foreach (var interpretation in interpretations.Elements(InterpretationTag))
            {
                var type = interpretation.Attribute(TypeTag)?.Value;
                if(type==null || type != "Clinical significance") continue;
                
                var description = interpretation.Element(DescriptionTag)?.Value.ToLower();
                var explanation = interpretation.Element(ExplanationTag)?.Value.ToLower();
                if(description == null && explanation == null) continue;

                var significances = ClinVarCommon.GetSignificances(description, explanation);
                foreach (var significance in significances)
                {
                    if (!ClinVarCommon.ValidPathogenicity.Contains(significance)) 
                        throw new InvalidDataException($"Invalid clinical significance found. Observed: {significance}");
                    significanceList.Add(significance);
                }
            }
            return significanceList;
        }

        public void Dispose()
        {
            _readStream?.Dispose();
        }
    }
}