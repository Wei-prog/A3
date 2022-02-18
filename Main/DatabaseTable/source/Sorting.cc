
#ifndef SORT_C
#define SORT_C

#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_Record.h"
#include "RecordComparator.h"
#include "Sorting.h"

using namespace std;

void mergeIntoFile(MyDB_TableReaderWriter &sortIntoMe, vector<MyDB_RecordIteratorAltPtr> &mergeUs,
									 function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) // Checked
{
	RecordComparator comp(comparator, lhs, rhs);
	std::priority_queue<MyDB_RecordIteratorAltPtr, vector<MyDB_RecordIteratorAltPtr>, RecordComparator> pq(comp); // Sort base on 1, store on 2, comparator in 3

	for (MyDB_RecordIteratorAltPtr recordPtr : mergeUs) // Get each record pointer
	{
		if (recordPtr->advance()) // If there are still something on that page
		{
			pq.push(recordPtr); // Push the record into the file
		}
	}

	while (pq.size() > 0)
	{
		MyDB_RecordIteratorAltPtr iter = pq.top();
		iter->getCurrent(lhs); // Use iterator to get next record
		sortIntoMe.append(lhs);
		pq.pop(); // Neet to remove from the queue
		// If there still records in the iterator
		if (iter->advance())
		{
			pq.push(iter);
		}
	}
}

// void mergeIntoFile(MyDB_TableReaderWriter &sortIntoMe, vector<MyDB_RecordIteratorAltPtr> &mergeUs,
// 									 function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) // Checked
// {
// 	RecordComparator comp(comparator, lhs, rhs);
// 	std::priority_queue<MyDB_RecordPtr, vector<MyDB_RecordPtr>, RecordComparator> pq(comp); // Sort base on 1, store on 2, comparator in 3

// 	for (MyDB_RecordIteratorAltPtr recordPtr : mergeUs)
// 	{
// 		while (recordPtr->advance())
// 		{
// 			recordPtr->getCurrent(lhs);
// 			pq.push(lhs);
// 		}
// 	}

// 	while (pq.size() > 0)
// 	{
// 		MyDB_RecordPtr ptr = pq.top();

// 		pq.pop();

// 		sortIntoMe.append(ptr);

// 		// If there still records in the iterator
// 		// if (iter->advance())
// 		// {
// 		// 	pq.push(iter);
// 		// }
// 	}
// }

/*This is the function that used to add the reocrd on different page in each run into the same page. */
void appendTothePage(MyDB_PageReaderWriter &myPage, vector<MyDB_PageReaderWriter> &result, MyDB_RecordPtr myRecord, MyDB_BufferManagerPtr parent) // Checked
{
	if (!myPage.append(myRecord))
	{
		result.push_back(myPage); // Push the full page into the myPage

		MyDB_PageReaderWriter newPage(*parent);
		newPage.append(myRecord);
		myPage = newPage; // Use the newly created page for the future read and write.
	}
}
vector<MyDB_PageReaderWriter> mergeIntoList(MyDB_BufferManagerPtr parent, MyDB_RecordIteratorAltPtr leftIter,
																						MyDB_RecordIteratorAltPtr rightIter, function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
	MyDB_PageReaderWriter myPage(*parent);
	// returns only one merge
	vector<MyDB_PageReaderWriter> result;
	vector<MyDB_PageReaderWriter> twoInOne;
	// MyDB_PageReaderWriter resPage(myRam)
	// list1->getCurrent(lhs);
	// list2->getCurrent(rhs);
	//< true ; > false
	// RecordComparator myComp(comparator,lhs,rhs);
	while (1)
	{
		leftIter->getCurrent(lhs);
		rightIter->getCurrent(rhs);

		if (comparator())
		{
			// myPage.append(lhs);
			appendTothePage(myPage, result, lhs, parent);
			if (!leftIter->advance())
			{
				appendTothePage(myPage, result, rhs, parent);
				while (rightIter->advance())
				{
					rightIter->getCurrent(rhs);
					appendTothePage(myPage, result, rhs, parent);
				}
				break;
			}
		}
		else
		{
			// myPage.append(rhs);
			appendTothePage(myPage, result, rhs, parent);
			if (!rightIter->advance())
			{
				appendTothePage(myPage, result, lhs, parent);
				while (leftIter->advance())
				{
					leftIter->getCurrent(lhs);
					appendTothePage(myPage, result, lhs, parent);
				}
				break;
			}
		}
	}
	result.push_back(myPage);
	return result;
}

void sort(int runSize, MyDB_TableReaderWriter &sortMe, MyDB_TableReaderWriter &sortIntoMe,
					function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) // Checked
{
	vector<MyDB_RecordIteratorAltPtr> recordIter;
	// sort a run
	vector<vector<MyDB_PageReaderWriter> > eachRun;
	// int numberOfPage = sortMe.getNumPages();

	for (int i = 0; i < sortMe.getNumPages(); i++)
	{
		MyDB_PageReaderWriter sortedPage = *sortMe[i].sort(comparator, lhs, rhs); // Sort the record on each page
		vector<MyDB_PageReaderWriter> arrayPages;
		arrayPages.push_back(sortedPage);
		eachRun.push_back(arrayPages);

		if (eachRun.size() == runSize || i == sortMe.getNumPages() - 1)
		{
			while (eachRun.size() > 1)
			{
				// int size = eachRun.size();
				vector<MyDB_PageReaderWriter> pageList1 = eachRun.back();
				eachRun.pop_back();
				vector<MyDB_PageReaderWriter> pageList2 = eachRun.back();
				eachRun.pop_back();

				vector<MyDB_PageReaderWriter> mergedList = mergeIntoList(sortMe.getBufferMgr(), getIteratorAlt(pageList1), getIteratorAlt(pageList2), comparator, lhs, rhs);

				eachRun.push_back(mergedList);
			}
			recordIter.push_back(getIteratorAlt(eachRun[0]));
			eachRun.clear();
		}
	}

	mergeIntoFile(sortIntoMe, recordIter, comparator, lhs, rhs);
}

#endif
