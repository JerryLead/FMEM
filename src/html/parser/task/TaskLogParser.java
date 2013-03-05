package html.parser.task;

import java.util.Scanner;


import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import profile.task.mapper.Mapper;
import profile.task.reducer.Reducer;

import html.util.DateParser;
import html.util.HtmlFetcher;

public class TaskLogParser {
	
	public static void parseMapperLog (String logLink, Mapper mapper) {
		Document mapLogs = HtmlFetcher.getHtml(logLink);
		
		Element syslogPre = mapLogs.getElementsByTag("pre").last();
		String syslog[] = syslogPre.text().split("\\n");
		
		int i;
		/*
		 * 2012-10-10 14:59:39,727 INFO org.apache.hadoop.mapred.MapTask: io.sort.mb = 500
		 * 2012-10-10 14:59:40,005 INFO org.apache.hadoop.mapred.MapTask: data buffer = 398458880/498073600
		 * 2012-10-10 14:59:40,005 INFO org.apache.hadoop.mapred.MapTask: record buffer = 1310720/1638400
		 */
		for(i = 0; i < syslog.length; i++) {
			if(syslog[i].contains("io.sort.mb")) {
				int ioSortMb = Integer.parseInt(syslog[i].substring(syslog[i].lastIndexOf('=') + 2)); //500
				long mapperStartTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(','))); //2012-10-10 14:59:39
				//System.out.println("io.sort.mb = " + ioSortMb);
				//System.out.println("mapperStartTimeMS = " + mapperStartTimeMS);
				//System.out.println();
				mapper.setMapperStartTimeMS(mapperStartTimeMS);
				
			}
			
			else if(syslog[i].contains("data buffer")) {
				String[] dataBuffer = syslog[i].substring(syslog[i].lastIndexOf('=') + 2).split("/");
				long softBufferLimit = Long.parseLong(dataBuffer[0]); //398458880
				long kvbufferBytes = Long.parseLong(dataBuffer[1]); //498073600
				//System.out.println("softBufferLimit = " + softBufferLimit + ", kvbufferLen = " + kvbufferLen);
				mapper.getMapperBuffer().setDataBuffer(softBufferLimit, kvbufferBytes);
			}
			
			else if(syslog[i].contains("record buffer")) {
				String[] recordBuffer = syslog[i].substring(syslog[i].lastIndexOf('=') + 2).split("/");
				long softRecordLimit = Long.parseLong(recordBuffer[0]); //1310720
				long kvoffsetsLen = Long.parseLong(recordBuffer[1]); //1638400
				
				mapper.getMapperBuffer().setRecordBuffer(softRecordLimit, kvoffsetsLen);
				//System.out.println("softRecordLimit = " + softRecordLimit + ", kvoffsetsLen = " + kvoffsetsLen);
				//System.out.println();
				i++;
				break;
			}		
		}
		/*
		 * 2012-10-10 14:59:41,167 INFO org.apache.hadoop.mapred.MapTask: Spilling map output: record full = true
		 * 2012-10-10 14:59:41,168 INFO org.apache.hadoop.mapred.MapTask: bufstart = 0; bufend = 13585067; bufvoid = 498073600
		 * 2012-10-10 14:59:41,168 INFO org.apache.hadoop.mapred.MapTask: kvstart = 0; kvend = 1310720; length = 1638400
		 * 2012-10-10 14:59:42,859 INFO org.apache.hadoop.mapred.MapTask: Finished spill 0 <RecordsBeforeCombine = 1310720, 
		 * BytesBeforeSpill = 13585067, RecordAfterCombine = 161552, RawLength = 2492902, CompressedLength = 2492966>
		 * or
		 * 2012-10-10 17:29:31,088 INFO org.apache.hadoop.mapred.MapTask: Finished spill 2 without combine <Records = 62916, 
		 * BytesBeforeSpill = 6291600, RawLength = 6417448, CompressedLength = 916344>
		 */
		long startSpillTimeMS = 0;
		String reason = "";
		int spillid = -1;
		
		for (; i < syslog.length; i++) {

			if(syslog[i].contains("Spilling map output")) {
				reason = "record";
				if(syslog[i].contains("buffer"))
					reason = "buffer";				
				startSpillTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(','))); //2012-10-10 14:59:41
				//System.out.println("startSpillTimeMS = " + startSpillTimeMS + ", reason = " + reason);
				++spillid;
			
			} //end if(syslog[i].contains("Spilling map output")) {
		
			else if (syslog[i].contains("Starting flush of map output")) {
				reason = "flush";
				startSpillTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(','))); //2012-10-10 14:59:41
				//System.out.println("startSpillTimeMS = " + startSpillTimeMS + ", reason = " + reason);
				++spillid;
			}
			
			//2012-12-05 16:17:35,669 INFO org.apache.hadoop.mapred.MapTask: [PartInfos][Partition 0]<RecordsBeforeCombine = 94671, 
			//RawLengthBeforeMerge = 9656444, RecordsAfterCombine = 94671, RawLength = 9656444, CompressedLength = 1404774>
			else if(syslog[i].contains("PartInfos")) {
				long stopMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				int partitionIdEnd = Integer.parseInt(syslog[i].substring(syslog[i].indexOf("Partition") + 10, syslog[i].lastIndexOf(']')));
				
				String valueString = syslog[i].substring(syslog[i].lastIndexOf('<') + 1, syslog[i].lastIndexOf('>'));
				long[] values = extractLongNumber(valueString, 5);
				
				long RecordsBeforeCombine = values[0];
				long RawLengthBeforeMerge = values[1];
				long RecordsAfterCombine = values[2];
				long RawLength = values[3];
				long CompressedLength = values[4];
				
				mapper.getMerge().addBeforeMergeItem(stopMergeTimeMS, partitionIdEnd, 1, RawLengthBeforeMerge, RawLengthBeforeMerge);
				mapper.getMerge().addAfterMergeItem(stopMergeTimeMS, partitionIdEnd, RecordsBeforeCombine, RecordsAfterCombine, RawLength, CompressedLength);
			}
			
			else if(syslog[i].contains("Finished spill")) {
				int spillLoc = syslog[i].indexOf("spill") + 6;
				int spillId = Integer.parseInt(syslog[i].substring(spillLoc, syslog[i].indexOf(" ", spillLoc))); // 0
				assert(spillid == spillId);
				long stopSpillTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(','))); //2012-10-10 14:59:42
				String valuesStr = syslog[i].substring(syslog[i].indexOf('<') + 1, syslog[i].lastIndexOf('>'));
				//System.out.print("[" + stopSpillTimeMS + "][" + spillId + "]");
				
				if(syslog[i].contains("without combine")) {
					long[] values = extractLongNumber(valuesStr, 4);
					long Records = values[0];// 1310720 
					long BytesBeforeSpill = values[1]; //13585067
					long RawLength = values[2]; //2492902
					long CompressedLength = values[3]; //2492966
					//System.out.println("[without combine] " + "Records = " + Records + ", BytesBeforeSpill = " + BytesBeforeSpill
					//		+ ", RawLength = " + RawLength + ", CompressedLength = " + CompressedLength);
					//System.out.println();
					mapper.getSpill().addSpillItem(false, startSpillTimeMS, stopSpillTimeMS, reason, Records, BytesBeforeSpill, Records, 
							RawLength, CompressedLength);
				}
				else {
					long[] values = extractLongNumber(valuesStr, 5);
					long RecordsBeforeCombine = values[0]; // 1310720 
					long BytesBeforeSpill = values[1]; //13585067
					long RecordAfterCombine = values[2]; //161552
					long RawLength = values[3]; //2492902
					long CompressedLength = values[4]; //2492966
					
					//System.out.println("[with combine] " + "RecordsBeforeCombine = " + RecordsBeforeCombine + ", BytesBeforeSpill = "
					//		+ BytesBeforeSpill + ", RecordAfterCombine = " + RecordAfterCombine + ", RawLength = " + RawLength
					//		+ ", CompressedLength = " + CompressedLength);
					//System.out.println();
					mapper.getSpill().addSpillItem(true, startSpillTimeMS, stopSpillTimeMS, reason, RecordsBeforeCombine, BytesBeforeSpill,
							RecordAfterCombine, RawLength, CompressedLength);
					
				}
			}
			else if(syslog[i].contains("[BeforeMerge]")) {
				long startMergePhaseTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				//System.out.println("----------------------------------------------------------------");
				//System.out.println("startMergePhaseTimeMS = " + startMergePhaseTimeMS);
				mapper.setMergePhaseStartTimeMS(startMergePhaseTimeMS);
				break;
			}	
			
			else if(syslog[i].contains("done. And is in the process of commiting")) {
				break;
			}
		} //end for (; i < syslog.length; i++)
		
		/*
		 * 2012-10-10 15:00:45,065 INFO org.apache.hadoop.mapred.MapTask: [BeforeMerge][Partition 12]<SegmentsNum = 32, RawLength = 4852329, CompressedLength = 4852457>
		 * 2012-10-10 15:00:45,065 INFO org.apache.hadoop.mapred.Merger: Merging 32 sorted segments
		 * 2012-10-10 15:00:45,090 INFO org.apache.hadoop.mapred.Merger: Merging 5 intermediate segments out of a total of 32 <WriteRecords = 49602, RawLength = 760991, CompressedLength = 760995>
		 * 2012-10-10 15:00:45,141 INFO org.apache.hadoop.mapred.Merger: Merging 10 intermediate segments out of a total of 28 <WriteRecords = 96566, RawLength = 1476601, CompressedLength = 1476605>
		 * 2012-10-10 15:00:45,193 INFO org.apache.hadoop.mapred.Merger: Merging 10 intermediate segments out of a total of 19 <WriteRecords = 98732, RawLength = 1519801, CompressedLength = 1519805>
		 * 2012-10-10 15:00:45,195 INFO org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 10 segments left of total size: 4852285 bytes
		 * 2012-10-10 15:00:45,325 INFO org.apache.hadoop.mapred.MapTask: [AfterMergeAndCombine][Partition 12]<RecordsBeforeCombine = 316038, 
		 * RecordsAfterCombine = 120636, RawLength = 1986498, CompressedLength = 1986502>
		 */
		for(; i < syslog.length; i++) {
			if(syslog[i].contains("[BeforeMerge]")) {
				long startMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				int partitionIdStart = Integer.parseInt(syslog[i].substring(syslog[i].indexOf("Partition") + 10, syslog[i].lastIndexOf(']')));
				String valueStr = syslog[i].substring(syslog[i].lastIndexOf('<') + 1, syslog[i].lastIndexOf('>'));
				
				long[] values = extractLongNumber(valueStr, 3);
				int SegmentsNum = (int)values[0]; //32
				long RawLength = values[1]; //4852329
				long CompressedLength = values[2]; //4852457
				//System.out.println("[BeforeMerge][" + partitionIdStart + "][" + startMergeTimeMS + "] SegmentsNum = " +
				//		SegmentsNum + ", RawLength = " + RawLength + ", CompressedLength = " + CompressedLength);
				mapper.getMerge().addBeforeMergeItem(startMergeTimeMS, partitionIdStart, SegmentsNum, RawLength, CompressedLength);
				
				
				for(int j = i + 1; j < syslog.length; j++) {
					if(syslog[j].contains("intermediate segments")) {
						long mergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));
						int mergeSegmentsNum = Integer.parseInt(syslog[j].substring(syslog[j].indexOf("Merging") + 8, syslog[j].indexOf("intermediate") - 1));
						int totalSegmentsNum = Integer.parseInt(syslog[j].substring(syslog[j].lastIndexOf("of") + 3, syslog[j].lastIndexOf('<') -1));
						String valueString = syslog[j].substring(syslog[j].lastIndexOf('<') + 1, syslog[j].lastIndexOf('>'));
						values = extractLongNumber(valueString, 3);
						
						long writeRecords = values[0];
						long rawLengthInter = values[1];
						long compressedLengthInter = values[2];
						
						mapper.getMerge().addIntermediateMergeItem(mergeTimeMS, mergeSegmentsNum, totalSegmentsNum, writeRecords, 
								rawLengthInter, compressedLengthInter);
						//System.out.println("[" + mergeTimeMS + "] mergeSegmentsNum = " + mergeSegmentsNum + ", totalSegmentsNum = "
						//		+ totalSegmentsNum + ", WriteRecords = " + writeRecords + ", RawLength = " + rawLengthInter
						//		+ ", CompressedLength = " + compressedLengthInter); 
					}
					else if(syslog[j].contains("Down to the last merge-pass")) {
						long mergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));
						String valueString = syslog[j].substring(syslog[j].indexOf("Down"));
						values = extractLongNumber(valueString, 2);
						
						int lastMergePassSegmentsNum = (int)values[0];
						long lastMergePassTotalSize = values[1];
						
						mapper.getMerge().addLastMergePassItem(mergeTimeMS, lastMergePassSegmentsNum, lastMergePassTotalSize);
						//System.out.println("[" + mergeTimeMS + "] lastMergePassSegmentsNum = " + lastMergePassSegmentsNum
						//		+ ", lastMergePassTotalSize = " + lastMergePassTotalSize); 
					}
					else if(syslog[j].contains("AfterMergeAndCombine")) {
						long stopMergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));
						int partitionIdEnd = Integer.parseInt(syslog[j].substring(syslog[j].indexOf("Partition") + 10, syslog[j].lastIndexOf(']')));
						assert(partitionIdStart == partitionIdEnd);
						String valueString = syslog[j].substring(syslog[j].lastIndexOf('<') + 1, syslog[j].lastIndexOf('>'));
						values = extractLongNumber(valueString, 4);
						
						long RecordsBeforeMerge = values[0]; //316038 
						long RecordsAfterMerge = values[1]; //120636
						long RawLengthEnd = values[2]; //1986498
						long CompressedLengthEnd = values[3]; //1986502
						
						mapper.getMerge().addAfterMergeItem(stopMergeTimeMS, partitionIdEnd, RecordsBeforeMerge, RecordsAfterMerge, RawLengthEnd, CompressedLengthEnd);
						//System.out.println("[AfterMergeAndCombine][" + partitionIdEnd + "][" + stopMergeTimeMS + "] RecordsBeforeCombine = "
						//		+ RecordsBeforeCombine + ", RecordsAfterCombine = " + RecordsAfterCombine + ", RawLength = " + RawLengthEnd
						//		+ ", CompressedLength = " + CompressedLengthEnd);
						//System.out.println();
						i = j;
						break;
					}
				}
			}
			/*
			 * 2012-10-10 17:29:41,925 INFO org.apache.hadoop.mapred.TaskRunner: Task:attempt_201210101630_0003_m_000000_0 is done. And is in the process of commiting
			 * 2012-10-10 17:29:41,929 INFO org.apache.hadoop.mapred.TaskRunner: Task 'attempt_201210101630_0003_m_000000_0' done.
			 */
			else if(syslog[i].contains("done. And is in the process of commiting")) {
				long mapperStopTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(','))); //2012-10-10 17:29:41
				//System.out.println("mapperStopTimeMS = " + mapperStopTimeMS);
				mapper.setMapperStopTimeMS(mapperStopTimeMS);
				break;
			}	
		}
		
	}
	
	public static void parseReducerLog (String logLink, Reducer reducer) {
		Document mapLogs = HtmlFetcher.getHtml(logLink);
		Element syslogPre = mapLogs.getElementsByTag("pre").last();
		String syslog[] = syslogPre.text().split("\\n");
		
		int i;
		for(i = 0; i < syslog.length; i++) {
			if(syslog[i].contains("ShuffleRamManager:")) { //ShuffleRamManager: MemoryLimit=652482944, MaxSingleShuffleLimit=163120736
				long reducerStartTime = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				
				int start = syslog[i].indexOf('=') + 1; //652482944, MaxSingleShuffleLimit=163120736
				int end = syslog[i].indexOf(',', start);//, MaxSingleShuffleLimit=163120736
				long memoryLimit = Long.parseLong(syslog[i].substring(start, end));
				
				start = syslog[i].indexOf('=', end) + 1; //163120736
				long maxSingleShuffleLimit = Long.parseLong(syslog[i].substring(start));
				
				reducer.setReducerStartTime(reducerStartTime);
				reducer.getReducerBuffer().set(memoryLimit, maxSingleShuffleLimit);
				//System.out.println("[" + reducerStartTime + "] MemoryLimit = " + memoryLimit + ", MaxSingleShuffleLimit = " + maxSingleShuffleLimit); 
				i++;
				break;
			}
		}
		
		for(; i < syslog.length; i++) {
			//2012-10-13 11:57:54,795 INFO org.apache.hadoop.mapred.ReduceTask: Shuffling 34476716 bytes (5047132 raw bytes) into RAM from attempt_201210131136_0002_m_000000_0
			if(syslog[i].contains("Shuffling")) {
				int start = syslog[i].indexOf("Shuffling") + 10; 
				int end = syslog[i].indexOf(' ', start); 
				long decompressedLen = Long.parseLong(syslog[i].substring(start, end)); //34476716
				
				start = syslog[i].indexOf('(', end) + 1; 
				end = syslog[i].indexOf(' ', start); 
				long compressedLen = Long.parseLong(syslog[i].substring(start, end)); //5047132
				
				start = syslog[i].indexOf("into", end) + 5; 
				end = syslog[i].indexOf(' ', start); 
				String storeLoc = syslog[i].substring(start, end); //RAM
				
				start = syslog[i].indexOf("from", end) + 5; 
				String sourceTaskId = syslog[i].substring(start); //attempt_201208242049_0014_m_000050_0
				
				long shuffleFinishTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				
				//System.out.println("[Shuffling][" + shuffleFinishTimeMS + "][" + storeLoc + "] decompressedLen = " + decompressedLen + ", compressedLen = " + compressedLen);
				reducer.getShuffle().addShuffleItem(shuffleFinishTimeMS, sourceTaskId, storeLoc, decompressedLen, compressedLen);
			}
			
			/*
			 *  LOG.info(reduceTask.getTaskID() + "We have  " + 
			 *  mapOutputFilesOnDisk.size() + " map outputs on disk. " +
			 *  "Triggering merge of " + ioSortFactor + " files");
			 */
			else if(syslog[i].contains("Triggering merge of")) {
				System.err.println("[Note] OnDiskMergeInShuffle is triggered!");
				long startMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));				
				String mergeLoc = "OnDiskShuffleMerge";
				reducer.getMergeInShuffle().addMergeInShuffleBeforeItem(mergeLoc, startMergeTimeMS);	
				
				for(int j = i + 1; j < syslog.length; j++) {
					if(syslog[j].contains("intermediate segments")) {
						long mergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));
						int mergeSegmentsNum = Integer.parseInt(syslog[j].substring(syslog[j].indexOf("Merging") + 8, syslog[j].indexOf("intermediate") - 1));
						int totalSegmentsNum = Integer.parseInt(syslog[j].substring(syslog[j].lastIndexOf("of") + 3, syslog[j].lastIndexOf('<') -1));
						String valueString = syslog[j].substring(syslog[j].lastIndexOf('<') + 1, syslog[j].lastIndexOf('>'));
						long[] values = extractLongNumber(valueString, 3);
						
						long writeRecords = values[0];
						long rawLengthInter = values[1];
						long compressedLengthInter = values[2];
						
						reducer.getMergeInShuffle().addShuffleIntermediateMergeItem(mergeLoc, mergeTimeMS, mergeSegmentsNum, totalSegmentsNum, writeRecords,
								rawLengthInter, compressedLengthInter);
						//System.out.println("[Intermediate][" + mergeTimeMS + "] mergeSegmentsNum = " + mergeSegmentsNum + ", totalSegmentsNum = "
						//		+ totalSegmentsNum + ", WriteRecords = " + writeRecords + ", RawLength = " + rawLengthInter
						//		+ ", CompressedLength = " + compressedLengthInter); 
					}
					else if(syslog[j].contains("Down to the last merge-pass")) {
						long mergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));
						String valueString = syslog[j].substring(syslog[j].indexOf("Down"));
						long[] values = extractLongNumber(valueString, 2);
						
						int lastMergePassSegmentsNum = (int)values[0];
						long lastMergePassTotalSize = values[1];
						
						reducer.getMergeInShuffle().addLastPassMergeInShuffleItem(mergeLoc, mergeTimeMS, lastMergePassSegmentsNum, lastMergePassTotalSize);
						//System.out.println("[" + mergeTimeMS + "] lastMergePassSegmentsNum = " + lastMergePassSegmentsNum
						//		+ ", lastMergePassTotalSize = " + lastMergePassTotalSize); 
					}
					
					/*
					 * LOG.info("[OnDiskShuffleMerge]<SegmentsNum = " + mapFiles.size() + ", "
					 * "Records = " +  totalRecordsBeforeCombine + ", "
					 * "BytesBeforeMerge = " + approxOutputSize + ", "
					 * "RawLength = " + writer.getRawLength() + ", "
					 * "CompressedLength = " + writer.getCompressedLength() + ">");
					 */
					else if(syslog[j].contains("[OnDiskShuffleMerge]")) {
						long stopMergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));	
						String valueStr = syslog[j].substring(syslog[j].indexOf('<') + 1, syslog[j].lastIndexOf('>'));
						long[] values = extractLongNumber(valueStr, 6);
						
						int SegmentsNum = (int)values[0];
						long Records = values[1];
						long BytesBeforeMerge = values[2];
						long RawLength = values[3];
						long CompressedLength = values[4];
				
						reducer.getMergeInShuffle().addShuffleAfterMergeItem(mergeLoc, stopMergeTimeMS, SegmentsNum, Records,
								BytesBeforeMerge, Records, RawLength, CompressedLength);
						//System.out.println("[InMemoryShuffleMerge][" + stopMergeTime + "] SegmentsNum = " + SegmentsNum + ", RecordsBeforeMergeAC = " + RecordsBeforeMergeAC
						//		+ ", BytesBeforeMergeAC = " + BytesBeforeMergeAC + ", RecordsAfterCombine = " + RecordsAfterCombine
						//		+ ", RawLength = " + RawLength + ", CompressedLength = " + CompressedLength);
						break;
					}
				}	
			}
			/*
			 * 2012-10-13 11:58:01,655 INFO org.apache.hadoop.mapred.ReduceTask: Initiating in-memory merge with 29 segments...
			 * 2012-10-13 11:58:01,657 INFO org.apache.hadoop.mapred.Merger: Merging 29 sorted segments
			 * 2012-10-13 11:58:01,657 INFO org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 29 segments left of total size: 998484484 bytes
			 * other lines
			 * 2012-10-13 11:58:42,240 INFO org.apache.hadoop.mapred.ReduceTask: [InMemoryShuffleMerge]<SegmentsNum = 29, RecordsBeforeMergeAC = 9789063, 
			 * BytesBeforeMergeAC = 998484484, RecordsAfterCombine = 9789063, RawLength = 998484428, CompressedLength = 149009950>
			 */
			else if(syslog[i].contains("Initiating in-memory merge")) {
				long startMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));				
				String mergeLoc = "InMemoryShuffleMerge";
				reducer.getMergeInShuffle().addMergeInShuffleBeforeItem(mergeLoc, startMergeTimeMS);
				
				for(int j = i + 1; j < syslog.length; j++) {
					if(syslog[j].contains("intermediate segments")) {
						long mergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));
						int mergeSegmentsNum = Integer.parseInt(syslog[j].substring(syslog[j].indexOf("Merging") + 8, syslog[j].indexOf("intermediate") - 1));
						int totalSegmentsNum = Integer.parseInt(syslog[j].substring(syslog[j].lastIndexOf("of") + 3, syslog[j].lastIndexOf('<') -1));
						String valueString = syslog[j].substring(syslog[j].lastIndexOf('<') + 1, syslog[j].lastIndexOf('>'));
						long[] values = extractLongNumber(valueString, 3);
						
						long writeRecords = values[0];
						long rawLengthInter = values[1];
						long compressedLengthInter = values[2];
						
						reducer.getMergeInShuffle().addShuffleIntermediateMergeItem(mergeLoc, mergeTimeMS, mergeSegmentsNum, totalSegmentsNum, writeRecords,
								rawLengthInter, compressedLengthInter);
						//System.out.println("[Intermediate][" + mergeTimeMS + "] mergeSegmentsNum = " + mergeSegmentsNum + ", totalSegmentsNum = "
						//		+ totalSegmentsNum + ", WriteRecords = " + writeRecords + ", RawLength = " + rawLengthInter
						//		+ ", CompressedLength = " + compressedLengthInter); 
					}
					else if(syslog[j].contains("Down to the last merge-pass")) {
						long mergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));
						String valueString = syslog[j].substring(syslog[j].indexOf("Down"));
						long[] values = extractLongNumber(valueString, 2);
						
						int lastMergePassSegmentsNum = (int)values[0];
						long lastMergePassTotalSize = values[1];
						
						reducer.getMergeInShuffle().addLastPassMergeInShuffleItem(mergeLoc, mergeTimeMS, lastMergePassSegmentsNum, lastMergePassTotalSize);
						//System.out.println("[" + mergeTimeMS + "] lastMergePassSegmentsNum = " + lastMergePassSegmentsNum
						//		+ ", lastMergePassTotalSize = " + lastMergePassTotalSize); 
					}
					else if(syslog[j].contains("[InMemoryShuffleMerge]")) {
						long stopMergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));	
						String valueStr = syslog[j].substring(syslog[j].indexOf('<') + 1, syslog[j].lastIndexOf('>'));
						long[] values = extractLongNumber(valueStr, 6);
						
						int SegmentsNum = (int)values[0]; //29
						long RecordsBeforeMergeAC = values[1];//9789063
						long BytesBeforeMergeAC = values[2]; //998484484
						long RecordsAfterCombine = values[3]; //9789063
						long RawLength = values[4]; //998484428
						long CompressedLength = values[5]; //149009950
				
						reducer.getMergeInShuffle().addShuffleAfterMergeItem(mergeLoc, stopMergeTimeMS, SegmentsNum, RecordsBeforeMergeAC,
								BytesBeforeMergeAC, RecordsAfterCombine, RawLength, CompressedLength);
						//System.out.println("[InMemoryShuffleMerge][" + stopMergeTime + "] SegmentsNum = " + SegmentsNum + ", RecordsBeforeMergeAC = " + RecordsBeforeMergeAC
						//		+ ", BytesBeforeMergeAC = " + BytesBeforeMergeAC + ", RecordsAfterCombine = " + RecordsAfterCombine
						//		+ ", RawLength = " + RawLength + ", CompressedLength = " + CompressedLength);
						break;
					}
				}		
			}
			//copy phase ends, there are some files left on disk and some segments in memory
			//2012-10-13 12:09:20,631 INFO org.apache.hadoop.mapred.ReduceTask: Interleaved on-disk merge complete: 12 files left.
			else if(syslog[i].contains("Interleaved on-disk merge complete")) {
				int mapOutputFilesOnDisk = Integer.parseInt(syslog[i].substring(
						syslog[i].lastIndexOf(':') + 2, syslog[i].lastIndexOf("files") - 1));
				//System.out.println("[on-disk merge complete] " + mapOutputFilesOnDisk + " files left");
				reducer.getSort().setOnDiskSegmentsLeftAfterShuffle(mapOutputFilesOnDisk);
			}
			//copy phase ends, there are some files left on disk and some segments in memory
			//2012-10-13 12:09:49,451 INFO org.apache.hadoop.mapred.ReduceTask: In-memory merge complete: 3 files left.
			else if(syslog[i].contains("In-memory merge complete")) {
				long shufflePhaseFinishTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));	
				int mapOutputsFilesInMemory = Integer.parseInt(syslog[i].substring(
						syslog[i].lastIndexOf(':') + 2, syslog[i].lastIndexOf("files") - 1));
				//System.out.println("[Shuffle Phase finished][" + mapOutputsFilesInMemory + "]" + " files left");
				reducer.setShufflePhaseFinishTimeMS(shufflePhaseFinishTimeMS);
				reducer.getSort().setInMemorySegmentsLeftAfterShuffle(mapOutputsFilesInMemory);
				
				i++;
				break;
			}
		}

		//sort phase begins		
		/*
		 * [InMemorySortMerge]
		 * 2012-10-13 11:42:18,676 INFO org.apache.hadoop.mapred.ReduceTask: In-memory merge complete: 38 files left.
		 * 2012-10-13 11:42:18,686 INFO org.apache.hadoop.mapred.Merger: Merging 38 sorted segments
		 * 2012-10-13 11:42:18,686 INFO org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 38 segments left of total size: 82317689 bytes
		 * 2012-10-13 11:42:21,960 INFO org.apache.hadoop.mapred.ReduceTask: [InMemorySortMerge]<SegmentsNum = 38, Records = 5083386, BytesBeforeMerge = 82317689,
 		 * RawLength = 82317615, CompressedLength = 82317619>
 		 * 
 		 * or
 		 * 
 		 * 2012-10-13 12:09:49,451 INFO org.apache.hadoop.mapred.ReduceTask: In-memory merge complete: 3 files left.
 		 * 2012-10-13 12:09:49,453 INFO org.apache.hadoop.mapred.ReduceTask: Keeping 3 segments, 103346202 bytes in memory for intermediate, on-disk merge
		 */
		
		for(; i < syslog.length; i++) {
			//2012-10-13 12:09:49,453 INFO org.apache.hadoop.mapred.ReduceTask: Keeping 3 segments, 103346202 bytes in memory for intermediate, on-disk merge
			if(syslog[i].contains("ReduceTask: Keeping")) {
				long stopInMemorySortMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				int mergeSegmentsNum = Integer.parseInt(syslog[i].substring(
						syslog[i].indexOf("Keeping") + 8, syslog[i].indexOf("segments") - 1));
				long sizeAfterMerge = Long.parseLong(syslog[i].substring(
						syslog[i].indexOf("segments,") + 10, syslog[i].lastIndexOf("bytes") - 1));
				//System.out.println("[Keeping in memory][" + stopInMemorySortMergeTimeMS + "] SegmentsNum = " 
				//		+ mergeSegmentsNum + ", sizeAfterMerge = " + sizeAfterMerge);
				
				reducer.getSort().setInMemorySortMergeItem(stopInMemorySortMergeTimeMS, mergeSegmentsNum, 0, sizeAfterMerge,
						sizeAfterMerge, sizeAfterMerge);
				i++;
				break;
			}
			//2012-10-13 11:42:21,960 INFO org.apache.hadoop.mapred.ReduceTask: [InMemorySortMerge]<SegmentsNum = 38, Records = 5083386, BytesBeforeMerge = 82317689,
	 		// RawLength = 82317615, CompressedLength = 82317619>
			else if (syslog[i].contains("[InMemorySortMerge]")) {
				long stopInMemorySortMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				String valueStr = syslog[i].substring(syslog[i].indexOf('<') + 1, syslog[i].lastIndexOf('>'));
				
				long[] values = extractLongNumber(valueStr, 5);
				int SegmentsNum = (int)values[0]; //38
				long Records = values[1]; //5083386
				long BytesBeforeMerge = values[2]; //82317689
				long RawLength = values[3]; //82317615
				long CompressedLength = values[4]; //82317619
				
				//System.out.println("[InMemorySortMerge][" + stopInMemorySortMergeTimeMS + "] SegmentsNum = " + SegmentsNum
				//		+ ", Records = " + Records + ", BytesBeforeMerge = " + BytesBeforeMerge
				//		+ ", RawLength = " + RawLength 
				//		+ ", CompressedLength = " + CompressedLength);
				
				reducer.getSort().setInMemorySortMergeItem(stopInMemorySortMergeTimeMS, SegmentsNum, Records, BytesBeforeMerge,
						RawLength, CompressedLength);
				i++;
				break;
			}
			
			else if (syslog[i].contains("[MixSortMerge]")) 
				break;
		}
		/*
		 * 2012-10-13 11:42:21,961 INFO org.apache.hadoop.mapred.Merger: Merging 1 sorted segments
		 * 2012-10-13 11:42:21,963 INFO org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 82317615 bytes
		 * 2012-10-13 11:42:21,963 INFO org.apache.hadoop.mapred.ReduceTask: [MixSortMerge][CountersBeforeMerge]<InMemorySegmentsNum = 0, InMemorySegmentsSize = 0, 
		 * OnDiskSegmentsNum = 1, OnDiskSegmentsSize = 82317619
		 * 
		 * or
		 * 
		 * 2012-10-13 12:09:49,458 INFO org.apache.hadoop.mapred.Merger: Merging 16 sorted segments
		 * 2012-10-13 12:12:12,869 INFO org.apache.hadoop.mapred.Merger: Merging 7 intermediate segments out of a total of 16 <WriteRecords = 38827533, RawLength = 3960408368, CompressedLength = 591102891>
		 * 2012-10-13 12:12:12,915 INFO org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 10 segments left of total size: 1913030519 bytes
		 * 2012-10-13 12:12:12,915 INFO org.apache.hadoop.mapred.ReduceTask: [MixSortMerge][CountersBeforeMerge]<InMemorySegmentsNum = 3, InMemorySegmentsSize = 103346202, 
		 * OnDiskSegmentsNum = 13, OnDiskSegmentsSize = 1896494449
		 */
		for(; i < syslog.length; i++) {
			if(syslog[i].contains("intermediate segments")) {
				long mergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				int mergeSegmentsNum = Integer.parseInt(syslog[i].substring(syslog[i].indexOf("Merging") + 8, syslog[i].indexOf("intermediate") - 1));
				int totalSegmentsNum = Integer.parseInt(syslog[i].substring(syslog[i].lastIndexOf("of") + 3, syslog[i].lastIndexOf('<') -1));
				String valueString = syslog[i].substring(syslog[i].lastIndexOf('<') + 1, syslog[i].lastIndexOf('>'));
				long[] values = extractLongNumber(valueString, 3);
				
				long writeRecords = values[0];
				long rawLengthInter = values[1];
				long compressedLengthInter = values[2];
				reducer.getSort().addIntermediateMergeInSortItem("MixSortMerge", mergeTimeMS, mergeSegmentsNum, totalSegmentsNum,
						writeRecords, rawLengthInter, compressedLengthInter);
				//System.out.println("[Intermediate][" + mergeTimeMS + "] mergeSegmentsNum = " + mergeSegmentsNum + ", totalSegmentsNum = "
				//		+ totalSegmentsNum + ", WriteRecords = " + writeRecords + ", RawLength = " + rawLengthInter
				//		+ ", CompressedLength = " + compressedLengthInter); 
			}
			else if(syslog[i].contains("Down to the last merge-pass")) {
				long mergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				String valueString = syslog[i].substring(syslog[i].indexOf("Down"));
				long[] values = extractLongNumber(valueString, 2);
				
				int lastMergePassSegmentsNum = (int)values[0];
				long lastMergePassTotalSize = values[1];
				
				reducer.getSort().addLastPassMergeInSortItem("MixSortMerge", mergeTimeMS, lastMergePassSegmentsNum, lastMergePassTotalSize);
				//System.out.println("[" + mergeTimeMS + "] lastMergePassSegmentsNum = " + lastMergePassSegmentsNum
				//		+ ", lastMergePassTotalSize = " + lastMergePassTotalSize); 
			}
			//2012-10-13 12:12:12,915 INFO org.apache.hadoop.mapred.ReduceTask: [MixSortMerge][CountersBeforeMerge]<InMemorySegmentsNum = 3, 
			//InMemorySegmentsSize = 103346202, OnDiskSegmentsNum = 13, OnDiskSegmentsSize = 1896494449
			else if (syslog[i].contains("[MixSortMerge]")) {
				long stopMixSortMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				//should be
				//String valuStr = syslog[i].substring(syslog[i].indexOf('<') + 1, syslog[i].lastIndexOf('>'));
				String valueStr = syslog[i].substring(syslog[i].indexOf('<') + 1);
						
				long[] values = extractLongNumber(valueStr, 4);
				int InMemorySegmentsNum = (int)values[0]; //3 
				long InMemorySegmentsSize = values[1]; //103346202
				int OnDiskSegmentsNum = (int)values[2]; //13
				long OnDiskSegmentsSize = values[3]; //1896494449
				
				reducer.getSort().addMixSortMergeItem(stopMixSortMergeTimeMS, InMemorySegmentsNum, InMemorySegmentsSize,
						OnDiskSegmentsNum, OnDiskSegmentsSize);
				reducer.setSortPhaseFinishTimeMS(stopMixSortMergeTimeMS);
				//System.out.println("[MixSortMerge][" + stopMixSortMergeTimeMS + "] InMemorySegmentsNum = " + InMemorySegmentsNum
				//		+ ", InMemorySegmentsSize = " + InMemorySegmentsSize + ", OnDiskSegmentsNum = " + OnDiskSegmentsNum
				//		+ ", OnDiskSegmentsSize = " + OnDiskSegmentsSize);
				i++;
				break;
			}
		}
		
		for(; i < syslog.length; i++) {
			//LOG.info("[FinalSortMerge]" + "<InMemorySegmentsNum = " + inMemSegmentsNum + ", "
		  	//+ "InMemorySegmentsSize = " + inMemBytes + ">");	
			if (syslog[i].contains("[FinalSortMerge]")) {
				long stopFinalSortMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				
				String valueStr = syslog[i].substring(syslog[i].indexOf('<') + 1);
				long[] values = new long[2];
				
				int InMemorySegmentsNum = (int)values[0]; //inMemSegmentsNum
				long inMemBytes = values[1]; //inMemBytes		
				reducer.getSort().addFinalSortMergeItem(stopFinalSortMergeTimeMS, InMemorySegmentsNum, inMemBytes);
				reducer.setSortPhaseFinishTimeMS(stopFinalSortMergeTimeMS);
			}
			//2012-10-13 12:15:15,245 INFO org.apache.hadoop.mapred.TaskRunner: Task:attempt_201210131136_0002_r_000000_0 is done. And is in the process of commiting
			//2012-10-13 12:15:17,250 INFO org.apache.hadoop.mapred.TaskRunner: Task attempt_201210131136_0002_r_000000_0 is allowed to commit now
			//2012-10-13 12:15:17,278 INFO org.apache.hadoop.mapred.FileOutputCommitter: Saved output of task 'attempt_201210131136_0002_r_000000_0' to hdfs://master:9000/Output/terasort-100GB
			//2012-10-13 12:15:17,280 INFO org.apache.hadoop.mapred.TaskRunner: Task 'attempt_201210131136_0002_r_000000_0' done.
			else if (syslog[i].contains("And is in the process of commiting")) {
				long reducePhaseStopTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));	
				
				//System.out.println("reducePhaseStopTimeMS = " + reducePhaseStopTimeMS);
				reducer.setReducePhaseStopTimeMS(reducePhaseStopTimeMS);
			}
			
			else if (syslog[i].contains("is allowed to commit now")) {
				long commitStartTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				reducer.setCommitStartTimeMS(commitStartTimeMS);
			}
			
			else if (i == syslog.length - 1) {
				long reducerStopTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				
				reducer.setReducerStopTimeMS(reducerStopTimeMS);
				//System.out.println("reducerStopTimeMS = " + reducerStopTimeMS);
			}
		}
	}
	
	public static long[] extractLongNumber(String line, int n) {
		long[] values = new long[n];
		Scanner scanner = new Scanner(line).useDelimiter("[^0-9]+");
		int i = 0;
		
		while(scanner.hasNextLong())
			values[i++] = scanner.nextLong();
		return values;
	}
	
	public static void main(String[] args) {
		//parseMapperLog("http://slave5:50060/tasklog?taskid=attempt_201210131136_0001_m_000000_0&all=true", null);
		//parseReducerLog("http://slave7:50060/tasklog?taskid=attempt_201210131136_0002_r_000000_0&all=true", null);
		parseReducerLog("http://slave6:50060/tasklog?taskid=attempt_201210131136_0001_r_000000_0&all=true", null);
	}

}
