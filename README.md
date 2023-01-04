# Yellow Temple Project

## Overview 
The Family Tree at familysearch.org provides the primary way in which information is gathered to provide temple ordinances to the ancestors and relatives of members of the Church of Jesus Christ of Latter-day Saints. The Family Tree requires a set of basic information about an individual in order for their temple work to be complete. This information includes a name and the date and place of one life event (birth, marriage, or death). If any of this information is missing the temple tab for the individual will display a "yellow temple" and indicate that more information is required for the ordinance to be completed. Once that required information is entered on the Family Tree then the yellow temple will turn into a green temple.

There are 320 million individuals on the Family Tree that have at least one yellow temple. This project has been designed to address these yellow temples in FamilySearch's database and convert them to green temples. The information needed to convert these database entries into green temples is available through data scraping
but has been unintentionally neglected by users of Family Search. 

Here is an example of a database person entry that is classified as a yellow temple. The red circle shows this persons birthdate but the birth place is missing. 
<img src="https://i.ibb.co/qFDNSwV/Group-8-3.png" alt="Group-8-3" border="0">

However, the missing information to convert this user profile to a green temple can be found here in a US Census record that has already been atttached to the profile. This persons birthplace is shown with a red underline as Delaware, United States. 
<img src="https://i.ibb.co/SNJM7Gj/Group-9-1.png" alt="Group-9-1" border="0">

## Solution 

To solve this problem we first begin with those profiles that are linked to a US Census. The following logic is then applied: 
 * Check if person data is incomplete ( missing birth or death place/date) 
 * Get every record connected to the person db entry 
 * For every record check the birth/death information that is missing from the person 
 * If every instance of birth/death information matches, prepare the data for upload to the database
 
The above is an oversimplification of the process used for converting these yellow temple entry to green ones. To retrieve and process all of this data requires 
API calls to Family Search's database and is done in accordance to the guidelines they have set for me. In this repository and specifically in yellowtemples.py and fullpipeline.py is an abstraction of the code used to perform these API calls, process and clean the data, and eventually autonomously update these database entries. Many different variations and versions of this code have been written as Family Search's criteria and goals have changed. 

## Results

This process has been used to successfully update and fix over 10,000 people entries for FamilySearch’s database. My algorithm will continue to be used in the future and is estimated to autonomously correct 2 million database edits. Additionally, I hope to be able to identify database person entries that can be processed manually by volunteers within my organization to correct the entries that cannot be done accurately by autonomous means. Currently I am coauthoring an academic paper to be published that will give further detail and data to my project. 


## Contact

Kaleb Rigg - kaleb2323rigg@gmail.com 

Project Link: [https://github.com/kalebrigg/MeetAndEat](https://github.com/kalebrigg/MeetAndEat.git)


<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/NicolasBrondin/basic-readme-template.svg?style=flat-square
[contributors-url]: https://github.com/NicolasBrondin/basic-readme-template/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/NicolasBrondin/basic-readme-template.svg?style=flat-square
[forks-url]: https://github.com/NicolasBrondin/basic-readme-template/network/members
[stars-shield]: https://img.shields.io/github/stars/NicolasBrondin/basic-readme-template.svg?style=flat-square
[stars-url]: https://github.com/NicolasBrondin/basic-readme-template/stargazers
[issues-shield]: https://img.shields.io/github/issues/NicolasBrondin/basic-readme-template.svg?style=flat-square
[issues-url]: https://github.com/NicolasBrondin/basic-readme-template/issues
[license-shield]: https://img.shields.io/github/license/NicolasBrondin/basic-readme-template.svg?style=flat-square
[license-url]: https://github.com/NicolasBrondin/basic-readme-template/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=flat-square&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/othneildrew
[product-screenshot]: docs/cover.jpg
Footer
© 2022 GitHub, Inc.
Footer navigation
Terms
Privacy
Security
Status
Docs
Contact GitHub
Pricing
API
Training
Blog
About

