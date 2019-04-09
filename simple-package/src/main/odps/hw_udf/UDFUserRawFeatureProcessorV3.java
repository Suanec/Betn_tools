package com.weibo.aliyun.udf.user_feed;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.weibo.aliyun.udf.common.Configs;
import com.weibo.aliyun.udf.common.Tag;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.weibo.aliyun.udf.common.Utils.*;

@Resolve({"string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string->string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string"})

public class UDFUserRawFeatureProcessorV3 extends UDTF {
	ExecutionContext ctx;

//	private boolean isLocalDebugMode = true;
	private boolean isLocalDebugMode = false;

	private static Map<String, String> user_interest_cold_start = new HashMap<>();
	private static Map<String, Integer> secondTagIdMap = new HashMap<>();
	private static Map<Integer, String> indexSecondTagMap = new HashMap<>();
	private static Set<String> thirdTagPrefixSet = new HashSet<>();
	private static Map<Integer, String> indexFirstTagMap = new HashMap<>();
	private int MAX_FIRST_TAG_INDEX = 37;
	private int MAX_SECOND_TAG_INDEX = 808;
	private double defaultClickRate = 0.064; // 10 / 157
	private double defaultRealClickRate = 0.035; // wilson(10 / 157)
	private double defaultInteractRate = 0.064;
	private double defaultRealInteractRate = 0.000175;

	private static Map<String, Integer> realExpoBornIndexMap = new HashMap<String, Integer>() {{
		put("70s", 0);
		put("80s", 1);
		put("90s", 2);
		put("00s", 3);
	}};

	private static Map<String, Integer> realExpoGenderIndexMap = new HashMap<String, Integer>() {{
		put("m", 0);
		put("f", 1);
	}};

	private Map<String, String> jsonToStringMap(String data) {
		GsonBuilder gb = new GsonBuilder();
		Gson g = gb.create();
		return g.fromJson(data, new TypeToken<Map<String, String>>() {
		}.getType());
	}

	private Map<String, Object> jsonToObjectMap(String data) {
		return new Gson().fromJson(data, new TypeToken<HashMap<String, Object>>() {
		}.getType());
	}

	private List<Map> jsonToList(String data) {
		GsonBuilder gb = new GsonBuilder();
		Gson g = gb.create();
		return g.fromJson(data, new TypeToken<List<Map>>() {
		}.getType());
	}

	private void loadDictConfigFile(String fileName) throws UDFException {
		try {
			InputStream in;
			if (isLocalDebugMode) {
				in = new FileInputStream(fileName);
			} else {
				in = ctx.readResourceFileAsStream(fileName);
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;
			while ((line = br.readLine()) != null) {
				String[] lineSplitted = line.split("\t", 2);
				String key = lineSplitted[0];
				String value = lineSplitted[1];
				user_interest_cold_start.put(key, value);
			}
		} catch (IOException e) {
			throw new UDFException(e);
		}
	}

	private Double genClickNumRate(double expo, double click, double defaultRate) {
		double rate = defaultRate;
		if (expo >= 1000 && click < expo) {
			rate = click / expo;
			rate = Math.min(1.0, rate);
		}
		return rate;
	}

	private String getMaxValueTag(Map<String, Double> tagMap, double validValue) {
		String resTag = "invalidValue";
		double resValue = 0.0;
		if (tagMap.size() > 0) {
			for (Map.Entry<String, Double> m : tagMap.entrySet()) {
				double curValue = m.getValue();
				if (curValue > resValue && curValue >= validValue) {
					resTag = m.getKey();
					resValue = curValue;
				}
			}
		}
		return resTag;
	}

	private Set<String> getCommonKeySet(Set<String> firstSet, Set<String> secondSet) {
		Set<String> commonKeySet = new HashSet<>();
		if (!firstSet.isEmpty() && !secondSet.isEmpty()) {
			for (String key : firstSet) {
				if (secondSet.contains(key)) {
					commonKeySet.add(key);
				}
			}
		}
		return commonKeySet;
	}

	private String getMaxMatchTag(Map<String, Double> userTagMap, Map<String, Tag> docTagMap, String recall_category, int tagType) {
		String resTag = "invalidValue";
		if (userTagMap.size() < 1 || docTagMap.size() < 1) {
			return resTag;
		}
		Set<String> commonKeySet = getCommonKeySet(userTagMap.keySet(), docTagMap.keySet());
		boolean is_category = checkIsValidFirstTag(recall_category);
		if (commonKeySet.size() > 0) {
			double tmp = 0;
			for (String s : commonKeySet) {
				double matchValue = userTagMap.get(s);
				switch (tagType) {
					case 1:
						if (!is_category || recall_category.equals(docTagMap.get(s).category)) {
							if (matchValue > tmp) {
								resTag = s;
								tmp = matchValue;
							}
						}
						break;
					case 2:
						if (!is_category || recall_category.equals(docTagMap.get(s).category)) {
							if (matchValue > tmp) {
								resTag = s;
								tmp = matchValue;
							}
						}
						break;
					case 3:
						if (!recall_category.equals(docTagMap.get(s).category) && !isPerson(s)) {
							matchValue *= 0.5;
						}
						if (matchValue > tmp) {
							resTag = s;
							tmp = matchValue;
						}
						break;
					default:
						break;
				}
			}
		}
		return resTag;
	}

	private String getMaxMatchTagV2(Map<String, Double> userTagMap, Map<String, Double> docTagMap) {
		String resTag = "invalidValue";
		if (userTagMap.size() < 1 || docTagMap.size() < 1) {
			return resTag;
		}
		Set<String> commonKeySet = getCommonKeySet(userTagMap.keySet(), docTagMap.keySet());
		if (commonKeySet.size() > 0) {
			double max_value = 0;
			for (String s : commonKeySet) {
				double matchValue = userTagMap.get(s);
				if (matchValue > max_value) {
					resTag = s;
					max_value = matchValue;
				}
			}
		}
		return resTag;
	}

	private boolean isPerson(String cate) {
		return cate.contains("Person_");
	}

	private int getContentForm(int picNum, int videoNum) {
		int ind = 10;
		int picMaxNum = Math.min(picNum, 9);
		if (picMaxNum > 0) {
			ind = picMaxNum - 1;
		} else if (videoNum > 0) {
			ind = 9;
		}
		return ind;
	}

	private Double getClickRate(double expo, double click, double defaultRate) {
		double rate = defaultRate;
		if (expo >= 1000 && click < expo) {
			rate = click / expo;
			rate = Math.min(0.4, rate);
		}
		return rate;
	}

	private Double getInteractRate(double expo, double click, double defaultRate) {
		//default: 0.097
		double rate = defaultRate;
		if (click < expo) {
			rate = (click + 2) / (expo + 3140.0);
			rate *= 100;
			rate = Math.min(rate, 0.8);
		}
		return rate;
	}

	private String ParseIntimacy(String intimacyRaw, String uid) {
		try {
			Map<String, String> author_intimacy_map = new HashMap<>();
			String[] uidUnit = intimacyRaw.split("\\|");
			for (String s : uidUnit) {
				String[] sa = s.split("@");
				String cur_author_id = sa[0].startsWith("1042:") ? sa[0].substring(5) : sa[0];
				double value = parseValueDouble(sa[1]);
				if (value > 0) {
					author_intimacy_map.put(cur_author_id, sa[1]);
				}
			}
			return author_intimacy_map.getOrDefault(uid, "0");
		} catch (Exception e) {
			return "0";
		}
	}

	private String map2StringDoubleSequence(Map<String, Double> targetMap) {
		List<String> retList = new LinkedList<>();
		for (Map.Entry<String, Double> entry : targetMap.entrySet()) {
			retList.add(entry.getKey() + "@" + entry.getValue());
		}
		return String.join(",", retList);
	}

	private String map2StringStringSequence(Map<String, String> targetMap) {
		List<String> retList = new LinkedList<>();
		for (Map.Entry<String, String> entry : targetMap.entrySet()) {
			retList.add(entry.getKey() + "@" + entry.getValue().replace(",", "@"));
		}
		return String.join(",", retList);
	}

	// 1042015:city_20130001 (中间20是中国编号, 13是省, 00是空, 01是市)
	// 8008613010000000000 (80086开头是大陆, 80886开头台湾, 80其它开头是外国, 13是省, 01是市, 区, ...)
	private String getProvinceTag(String area_id, String... minning_province_tag) {
		String province_id = "unknown_province";
		if (minning_province_tag.length > 0 && !isEmptyString(minning_province_tag[0]) && minning_province_tag[0].startsWith("1042015:province_")) {
			province_id = minning_province_tag[0];
		}
		if (area_id.startsWith("1042015:province_")) {
			province_id = area_id;
		} else if (area_id.contains("city")) {
			int pos = area_id.indexOf("_");
			province_id = "1042015:province_" + area_id.substring(pos + 3, pos + 5);
		} else if (area_id.startsWith("80086")) {
			province_id = "1042015:province_" + area_id.substring(5, 7);
		} else if (area_id.startsWith("80886")) { // 台湾
			province_id = "1042015:province_71";
		} else if (area_id.startsWith("80")) {
			province_id = "1042015:province_400";
		}
		return province_id;
	}

	private void loadDictConfigFileWithIndexNew(String fileName) throws UDFException {
		try {
			InputStream in;
			if (isLocalDebugMode) {
				in = new FileInputStream(fileName);
			} else {
				in = ctx.readResourceFileAsStream(fileName);
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;
			while ((line = br.readLine()) != null) {
				String[] lineSplitted = line.trim().split("=");
				if (lineSplitted.length == 2 && !lineSplitted[0].isEmpty()) {
					String tagName = lineSplitted[0];
					int tagId = Integer.valueOf(lineSplitted[1]);
					secondTagIdMap.put(tagName, tagId);
					indexSecondTagMap.put(tagId, tagName);
				}
			}
			MAX_SECOND_TAG_INDEX = secondTagIdMap.size();
			indexSecondTagMap.put(MAX_SECOND_TAG_INDEX, "default");
			secondTagIdMap.put("default", MAX_SECOND_TAG_INDEX);
			br.close();
			in.close();
		} catch (IOException e) {
			throw new UDFException(e);
		}
	}

	private void loadSetConfigFile(String fileName) throws UDFException {
		try {
			InputStream in;
			if (isLocalDebugMode) {
				in = new FileInputStream(fileName);
			} else {
				in = ctx.readResourceFileAsStream(fileName);
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;
			while ((line = br.readLine()) != null) {
				String cur_prefix = line.trim();
				if (!cur_prefix.isEmpty()) {
					thirdTagPrefixSet.add(cur_prefix);
				}
			}
			br.close();
			in.close();
		} catch (IOException e) {
			throw new UDFException(e);
		}
	}

	private void initConfig() {
		for (Map.Entry<String, Integer> entry : Configs.firstTagIndexMap.entrySet()) {
			indexFirstTagMap.put(entry.getValue(), entry.getKey());
		}
		MAX_FIRST_TAG_INDEX = Configs.firstTagIndexMap.size();
		indexFirstTagMap.put(MAX_FIRST_TAG_INDEX, "default");
	}

	private JsonObject getJsonObject(String json) {
		JsonParser jsonParser = new JsonParser();
		return jsonParser.parse(json).getAsJsonObject();
	}

	private String getJsonString(JsonObject jo, String key, String defaultValue) {
		JsonElement jsonElement = jo.get(key);
		if (jsonElement != null) {
			return jsonElement.getAsString();
		} else {
			return defaultValue;
		}
	}

	private int getJsonInt(JsonObject jo, String key, int defaultValue) {
		JsonElement jsonElement = jo.get(key);
		if (jsonElement != null) {
			return jsonElement.getAsInt();
		} else {
			return defaultValue;
		}
	}

	private double getJsonDouble(JsonObject jo, String key, double defaultValue) {
		JsonElement jsonElement = jo.get(key);
		if (jsonElement != null) {
			return jsonElement.getAsDouble();
		} else {
			return defaultValue;
		}
	}

	private JsonArray getJsonArray(JsonObject jo, String key, JsonArray defaultValue) {
		JsonElement jsonElement = jo.get(key);
		if (jsonElement != null) {
			return jsonElement.getAsJsonArray();
		} else {
			return defaultValue;
		}
	}

	private boolean checkIsValidFirstTag(String tagName) {
		return !tagName.isEmpty() && tagName.startsWith("1042015:") && tagName.contains("tagCategory") && Configs.firstTagIndexMap.containsKey(tagName);
	}

	private boolean checkIsValidSecondTag(String tagName) {
		return !tagName.isEmpty() && tagName.startsWith("1042015:") && tagName.contains("abilityTag") && secondTagIdMap.containsKey(tagName);
	}

	private boolean checkIsValidThirdTag(String tagName) {
		boolean status = false;
		if (!tagName.isEmpty() && tagName.startsWith("1042015:")) {
			int pos = tagName.indexOf("_", 0);
			String tagPrefix = tagName.substring(8, pos);
			if (thirdTagPrefixSet.contains(tagPrefix)) {
				status = true;
			}
		}
		return status;
	}

	private Map<String, Double> string2Map(String strTags, String... args) {
		Map<String, Double> resultMap = new HashMap<>();
		String type = args.length > 0 ? args[0] : "default";
		String[] tagValueArray = strTags.split(",");
		for (String item : tagValueArray) {
			String[] tag_value = item.split("@");
			if (tag_value.length == 2) {
				String tagName = parseValueString(tag_value[0]);
				tagName = tagName.replace("vertical_t_", "");
				double tagValue = parseValueDouble(tag_value[1]);
				if (!tagName.isEmpty() && tagValue > 0) {
					boolean isValidTagName;
					switch (type) {
						case "first": {
							isValidTagName = checkIsValidFirstTag(tagName);
							break;
						}
						case "second": {
							isValidTagName = checkIsValidSecondTag(tagName);
							break;
						}
						case "third": {
							isValidTagName = checkIsValidThirdTag(tagName);
							break;
						}
						default: {
							isValidTagName = true;
						}
					}
					if (isValidTagName) {
						resultMap.put(tagName, parseValueDouble(tag_value[1]));
					}
				}
			}
		}
		return resultMap;
	}

	private Map<String, Double> getUserInterestSecondAndThird(Map<String, Double> long_tags_map, Map<String, Double> short_tags_map) {
		if (!short_tags_map.isEmpty()) {
			for (Map.Entry<String, Double> entry : short_tags_map.entrySet()) {
				String tagName = entry.getKey();
				double tagValue = entry.getValue();
				if (long_tags_map.containsKey(tagName)) {
					long_tags_map.put(tagName, Math.max(long_tags_map.get(tagName) * 0.9, tagValue));
				} else {
					long_tags_map.put(tagName, tagValue);
				}
			}
		}
		return long_tags_map;
	}

	private Map<String, Double> getUserInterestFirst(Map<String, Double> cold_start_tags_map,
													 Map<String, Double> long_first_tags_map,
													 Map<String, Double> short_first_tags_map,
													 Map<String, Double> merged_long_tags_map) {
		Comparator<Map.Entry<String, Double>> MapValueComparator = Comparator.comparing(Map.Entry::getValue);

		int firstLevelCnt = long_first_tags_map.size();
		if (firstLevelCnt < 1) {
			for (Map.Entry<String, Double> entry : cold_start_tags_map.entrySet()) {
				long_first_tags_map.put(entry.getKey(), entry.getValue());
				merged_long_tags_map.put(entry.getKey(), entry.getValue());
			}
		} else {
			List<Map.Entry<String, Double>> first_user_inter_list = new ArrayList<>(long_first_tags_map.entrySet());
			// NOTE: sort 小大
			first_user_inter_list.sort(Comparator.comparing(Map.Entry::getValue));

			List<Map.Entry<String, Double>> user_inter_cold_start_list = new ArrayList<>(cold_start_tags_map.entrySet());
			// NOTE: sort 大小
			user_inter_cold_start_list.sort(MapValueComparator.reversed());

			// NOTE: merge冷启动规则
			double minWeight = first_user_inter_list.get(0).getValue();
			double top = Math.min(minWeight - 0.1, 0.5);
			double firstFill = user_inter_cold_start_list.get(0).getValue();
			if (firstFill < 0.0000001) {
				System.out.println("cold start interest, max value < 0");
				return long_first_tags_map;
			}
			Set<String> idSet = new HashSet<>(long_first_tags_map.keySet());
			boolean flag = false;
			for (Map.Entry<String, Double> entry : user_inter_cold_start_list) {
				if (!idSet.contains(entry.getKey())) {
					if (!flag) {
						long_first_tags_map.put(entry.getKey(), top);
						merged_long_tags_map.put(entry.getKey(), top);
						flag = true;
						firstFill = entry.getValue();
					} else {
						long_first_tags_map.put(entry.getKey(), entry.getValue() / firstFill * top);
						merged_long_tags_map.put(entry.getKey(), entry.getValue() / firstFill * top);
					}
					firstLevelCnt++;
					idSet.add(entry.getKey());
					if (firstLevelCnt >= 29) { // max cold start interest count
						break;
					}
				}
			}
		}

		if (!short_first_tags_map.isEmpty()) {
			List<Map.Entry<String, Double>> firstUserShortInterList = new ArrayList<>(short_first_tags_map.entrySet());
			// NOTE: sort 大小
			firstUserShortInterList.sort(MapValueComparator.reversed());
			for (int i = 0; i < firstUserShortInterList.size() && i < 6; ++i) {
				String tagName = firstUserShortInterList.get(i).getKey();
				if (long_first_tags_map.containsKey(tagName)) {
					double newWeight = long_first_tags_map.get(tagName) + firstUserShortInterList.get(i).getValue() * 0.7;
					newWeight = (newWeight > 1) ? 0.99 : newWeight;
					long_first_tags_map.put(tagName, newWeight);
				} else {
					long_first_tags_map.put(tagName, firstUserShortInterList.get(i).getValue() * 0.9);
				}
			}
		}
		return long_first_tags_map;
	}

	// 内容形式: 无, 视频, 短链, 长文, gif图, 长图, 全景图, 普通图 old
	// 内容形式: 无, 视频, 长文, gif图, 长图, 全景图, 普通图, 短链 new
	private int getContentFormType(int video_num, int link_num, int article_num, int pic_num, int gif_num,
								   int long_pic_num, int mblog_panorama_num) {
		int content_form_type = 0; // 全无
		if (video_num > 0) {
			content_form_type = 1;
		} else if (article_num > 0) {
			content_form_type = 2;
		} else if (gif_num > 0) {
			content_form_type = 3;
		} else if (long_pic_num > 0) {
			content_form_type = 4;
		} else if (mblog_panorama_num > 0) {
			content_form_type = 5;
		} else if (pic_num > 0) {
			content_form_type = 6;
		} else if (link_num > 0) {
			content_form_type = 7;
		}
		return content_form_type;
	}

	private Double getNormValue(Double value, Double coeff) {
		return (2 / (1 + Math.exp(coeff * value)) - 1);
	}

	private int getPictureNumIndex(int pictureNum) {
		if (pictureNum <= 0) {
			return 0;
		} else if (pictureNum >= 9) {
			return 9;
		} else {
			return pictureNum;
		}
	}

	private String getUserCityLevel(String userCityTag, String minning_city_level) {
		String c_level = "0";
		if (!isEmptyString(userCityTag) && !userCityTag.equals("userUnknownLocation") && Configs.city2LevelMap.containsKey(userCityTag)) {
			c_level = Configs.city2LevelMap.get(userCityTag);
		} else if (!isEmptyString(minning_city_level)) {
			c_level = minning_city_level;
		}
		return c_level;
	}

	private boolean isCityTagId(String tag) {
		return tag.startsWith("1042015:city_") || (tag.startsWith("1042015:province_") && Configs.municipalitySet.contains(tag));
	}

	private boolean isValidAreaId(String data) {
		return data.length() == 19 && data.startsWith("80") && Configs.area2CityExchangeMap.containsKey(data);
	}

	private String getUserCityTag(String location, String userLocation, String user_city_tag, String areaId, String defaultLocation) {
		String cityTagId = defaultLocation;
		boolean found = false;
		if (!isEmptyString(location)) {
			location = location.contains("_new") ? location.replace("_new", "") : location;
			if (isCityTagId(location)) {
				cityTagId = location;
				found = true;
			} else if (isValidAreaId(location)) {
				cityTagId = Configs.area2CityExchangeMap.get(location);
				found = true;
			}
		}
		if (!found && !isEmptyString(userLocation)) {
			userLocation = userLocation.contains("_new") ? userLocation.replace("_new", "") : userLocation;
			if (isCityTagId(userLocation)) {
				cityTagId = userLocation;
				found = true;
			} else if (isValidAreaId(userLocation)) {
				cityTagId = Configs.area2CityExchangeMap.get(userLocation);
				found = true;
			}
		}
		if (!found && !isEmptyString(user_city_tag)) {
			if (isCityTagId(user_city_tag)) {
				cityTagId = user_city_tag;
				found = true;
			}
		}
		if (!found && !isEmptyString(areaId) && isValidAreaId(areaId)) {
			cityTagId = Configs.area2CityExchangeMap.get(areaId);
			found = true;
		}
		return cityTagId;
	}

	private String getUserAreaId(String area_id, String location_id, String minning_extra_area_id, String defaultLocation) {
		String areaId = defaultLocation;
		boolean found = false;
		if (!isEmptyString(area_id) && area_id.length() == 19 && area_id.startsWith("80")) {
			areaId = area_id;
			found = true;
		}
		if (!found && !isEmptyString(location_id) && location_id.length() == 9 && location_id.startsWith("80")) {
			areaId = location_id + "0000000000";
			found = true;
		}
		if (!found && !isEmptyString(minning_extra_area_id) && minning_extra_area_id.length() == 19 && minning_extra_area_id.startsWith("80")) {
			areaId = minning_extra_area_id;
			found = true;
		}
		return areaId;
	}

	private String areaId2CityTag(String area_id, String defaultLocation) {
		String location_id = defaultLocation;
		area_id = area_id.contains("_new") ? area_id.replace("_new", "") : area_id;
		if (area_id.startsWith("1042015:province_") || area_id.startsWith("1042015:city_")) {
			location_id = area_id;
		} else if (area_id.startsWith("80086")) {
			// 0~9市
			location_id = Configs.area2CityExchangeMap.getOrDefault((area_id.substring(0, 9) + "0000000000"), defaultLocation);
		} else if (area_id.startsWith("80886")) { // 台湾
			location_id = "1042015:province_71";
		} else if (area_id.startsWith("80")) {
			location_id = "1042015:province_400";
		}
		return location_id;
	}

	private int getAuthorVerifiedType(String verifiedType, String userProperty) {
		int typeIndex = 3;
		if (userProperty.equals("1") || userProperty.equals("2")) { // 明星: 1小明星, 2大明星
			typeIndex = 0;
		} else if (verifiedType.equals("0")) { // 橙V
			typeIndex = 1;
		} else if (Integer.parseInt(verifiedType) >= 1 && Integer.parseInt(verifiedType) <= 7) { // 蓝V
			typeIndex = 2;
		}
		return typeIndex;
	}

	private String updateExposurePosition(String exposure_position, String mblogRank) {
		int pos = -1;
		int rank;
		if (!isEmptyString(mblogRank)) {
			rank = parseValueDouble(mblogRank).intValue();
			if (rank >= 0 && rank < 25) {
				pos = rank;
			} else if (!isEmptyString(exposure_position)) {
				rank = parseValueDouble(exposure_position).intValue();
				if (rank >= 0 && rank < 25) {
					pos = rank;
				}
			}
		}
		return String.valueOf(pos);
	}

	private String getExtend2Value(String rawValue, String extendValue, String... defaultValue) {
		String ret = (defaultValue.length > 0) ? defaultValue[0] : "";
		if (!isEmptyString(extendValue)) {
			ret = extendValue;
		} else if (!isEmptyString(rawValue)) {
			ret = rawValue;
		}
		return ret;
	}

	private double getBornGenderRealExpoNum(String born, String gender, String mblogTimeBornGenderRealRead, double defaultValue) {
		double ret = defaultValue;
		if (!isEmptyString(mblogTimeBornGenderRealRead)) {
			int bornGenderIndex = realExpoBornIndexMap.getOrDefault(born, 4) * 3 + realExpoGenderIndexMap.getOrDefault(gender, 2);
			String[] dataList = mblogTimeBornGenderRealRead.split(",");
			if (dataList.length == 30) {	// 前15位为历史真实曝光数,后15位为上个小时真实曝光数
				ret = parseValueDouble(dataList[bornGenderIndex]);
			}
		}
		return ret;
	}

	private void getCityLevelInfo(String cityLevel, String mblogCityLevelRealReadInfo, List<Double> infoList) {
		int cityLevelIndex = parseValueInteger(cityLevel);
		if (!isEmptyString(cityLevel) && cityLevelIndex > 0) {
			String[] dataList = mblogCityLevelRealReadInfo.split(",");
			if (dataList.length == 12) {
				cityLevelIndex = (cityLevelIndex > 4) ? 3 : cityLevelIndex - 1;
				infoList.set(0, parseValueDouble(dataList[cityLevelIndex]));
				infoList.set(1, parseValueDouble(dataList[cityLevelIndex + 4]));
				infoList.set(2, parseValueDouble(dataList[cityLevelIndex + 8]));
			}
		}
	}

	private int getBornIndex(String born) {
		int born_index = 4;
		switch (born) {
			case "70s": {
				born_index = 0;
				break;
			}
			case "80s": {
				born_index = 1;
				break;
			}
			case "90s": {
				born_index = 2;
				break;
			}
			case "00s": {
				born_index = 3;
				break;
			}
		}
		return born_index;
	}

	private int getGenderIndex(String gender) {
		int gender_index = 2;
		if (gender.equals("m")) {
			gender_index = 0;
		} else if (gender.equals("f")) {
			gender_index = 1;
		}
		return gender_index;
	}

	private double getWilsonValue(double expo_num, double click_num, double default_value) {
		double result = default_value;
		if (expo_num > 0 && expo_num > click_num) {
			double Z = 1.96;
			double p = click_num / expo_num;
			result = (p + Math.pow(Z, 2) / (2 * expo_num) - Z * Math.sqrt(p * (1 - p) / expo_num + Math.pow(Z, 2) / (4 * Math.pow(expo_num, 2))))
					/ (1 + Math.pow(Z, 2) / expo_num);
		}
		return result;
	}

	private int getTimePartIndex(String expo_time) {
		int index = 8;
		if (isEmptyString(expo_time) || (expo_time.length() != 10 && expo_time.length() != 13)) {
			return index;
		}
		try {
			SimpleDateFormat format = new SimpleDateFormat("HH");
			if (expo_time.length() == 10) {
				expo_time = expo_time + "000";
			}
			int hour = parseValueInteger(format.format(new Date(Long.valueOf(expo_time))));
			if (1 <= hour && hour < 8) {
				index = 0;
			} else if (8 <= hour && hour < 11) {
				index = 1;
			} else if (11 <= hour && hour < 14) {
				index = 2;
			} else if (14 <= hour && hour < 15) {
				index = 3;
			} else if (15 <= hour && hour < 18) {
				index = 4;
			} else if (18 <= hour && hour < 21) {
				index = 5;
			} else if (21 <= hour && hour < 23) {
				index = 6;
			} else if (23 <= hour || hour < 1) {
				index = 7;
			}
		} catch (Exception e) {
		}
		return index;
	}

	@Override
	public void setup(ExecutionContext ctx) throws UDFException {
		this.ctx = ctx;
		try {
			if (isLocalDebugMode) {
				// NOTE: for debug
//				loadDictConfigFile("/Users/Michael/dev_g/ubuntu/works/hot_weibo_aliyun_offline_workflow/conf/user_interest_cold_start_file");
//				loadDictConfigFileWithIndexNew("/Users/Michael/dev_g/ubuntu/works/hot_weibo_aliyun_offline_workflow/conf/second_tag_index.conf");
//				loadSetConfigFile("/Users/Michael/dev_g/ubuntu/works/hot_weibo_aliyun_offline_workflow/conf/third_tag_object_prefix.conf");

				loadDictConfigFile("/Users/wenbin9/works/tmp/user_interest_cold_start_file");
				loadDictConfigFileWithIndexNew("/Users/wenbin9/works/tmp/second_tag_index.conf");
				loadSetConfigFile("/Users/wenbin9/works/tmp/third_tag_object_prefix.conf");
			} else {
				loadDictConfigFile("user_interest_cold_start_file");
				loadDictConfigFileWithIndexNew("second_tag_index.conf");
				loadSetConfigFile("third_tag_object_prefix.conf");
			}
		} catch (Exception e) {
			throw new UDFException(e);
		}
		initConfig();
	}

	@Override
	public void process(Object[] args) throws UDFException {
		String is_click = parseValueString(args[0]);
		String actions = parseValueString(args[1]);
		String isautoplay = parseValueString(args[2]);
		String expo_time = parseValueString(args[3]);
		String network_type = parseValueString(args[4]);
		String recommend_source = parseValueString(args[5]);
		String recall_category = parseValueString(args[6]);
		String real_duration = parseValueString(args[7]);
		String exposure_position = parseValueString(args[8]);
		String effect_weight = parseValueString(args[9]);
		String uid = parseValueString(args[10]);
		String user_frequency = parseValueString(args[11]);
		String user_born = parseValueString(args[12]);
		String user_gender = parseValueString(args[13]);
		String request_area_id = parseValueString(args[14]);
		String match_first_level_inte_weight = parseValueString(args[15]);
		String match_second_level_inte_weight = parseValueString(args[16]);
		String match_third_level_inte_weight = parseValueString(args[17]);
		String user_long_interests = parseValueString(args[18]);
		String user_short_interests = parseValueString(args[19]);
		String user_intimacy = parseValueString(args[20]);
		String user_minning_city_level = parseValueString(args[21]);
		String user_minning_extra_area_id = parseValueString(args[22]);
		String user_minning_city_name = parseValueString(args[23]);
		String user_minning_city_tag = parseValueString(args[24]);
		String user_minning_province_name = parseValueString(args[25]);
		String user_minning_province_tag = parseValueString(args[26]);
		String user_minning_city_weight = parseValueString(args[27]);
		String author_id = parseValueString(args[28]);
		String author_verified_type = parseValueString(args[29]);
		String author_class = parseValueString(args[30]);
		String author_property = parseValueString(args[31]);
		String author_gender = parseValueString(args[32]);
		String author_city = parseValueString(args[33]);
		String author_province = parseValueString(args[34]);
		String author_followers_num = parseValueString(args[35]);
		String author_statuses_count = parseValueString(args[36]);
		String mid = parseValueString(args[37]);
		double mblog_ret_num = parseValueDouble(args[38]);
		double mblog_cmt_num = parseValueDouble(args[39]);
		double mblog_like_num = parseValueDouble(args[40]);
		String mblog_ret_num_recent = parseValueString(args[41]);
		String mblog_cmt_num_recent = parseValueString(args[42]);
		String mblog_like_num_recent = parseValueString(args[43]);
		double mblog_expose_num = parseValueDouble(args[44]);
		double mblog_act_num = parseValueDouble(args[45]);
		String mblog_expose_num_recent = parseValueString(args[46]);
		String mblog_act_num_recent = parseValueString(args[47]);
		String mblog_article_read_num = parseValueString(args[48]);
		String mblog_miaopai_view_num = parseValueString(args[49]);
		String mblog_text_len = parseValueString(args[50]);
		String content_tag = parseValueString(args[51]);
		String mblog_level = parseValueString(args[52]);
		String mblog_topic_num = parseValueString(args[53]);
		int mblog_gif_num = parseValueInteger(args[54]);
		int mblog_long_pic_num = parseValueInteger(args[55]);
		int mblog_pic_num = parseValueInteger(args[56]);
		int mblog_miaopai_num = parseValueInteger(args[57]);
		int mblog_link_num = parseValueInteger(args[58]);
		int mblog_article_num = parseValueInteger(args[59]);
		String mblog_title_num = parseValueString(args[60]);
		String dictionary = parseValueString(args[61]);
		String v_valid_play_duration = parseValueString(args[62]);
		String v_object_duration = parseValueString(args[63]);
		String v_duration = parseValueString(args[64]);
		String v_replay_count = parseValueString(args[65]);
		String v_video_orientation = parseValueString(args[66]);
		String extend = parseValueString(args[67]);
		String extend2 = parseValueString(args[68]);
		String extend3 = parseValueString(args[69]);
		String dt = parseValueString(args[70]);

		JsonObject extend_info;
		try {
			extend_info = getJsonObject(extend);
		} catch (Exception e) {
			return;
		}

		Map<String, String> extend2Map;
		try {
			String extend2Real;
			if (extend2.contains("##")) {
				extend2Real = extend2.substring(extend2.lastIndexOf("#") + 1);
			} else {
				extend2Real = extend2;
			}
			extend2Map = jsonToStringMap(jsonToObjectMap(extend2Real).get("doc_dictionary").toString());
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		if (isEmptyString(request_area_id)) {
			request_area_id = "unknown_area_id";
		}

		if ((recall_category.contains("1042015:city_") || recall_category.contains("1042015:province_")) && recall_category.contains("_new")) {
			recall_category = recall_category.replace("_new", "");
		}

		int recall_category_id = Configs.firstTagIndexMap.getOrDefault(recall_category, MAX_FIRST_TAG_INDEX);
		if (recall_category_id <= 1 || recall_category_id == MAX_FIRST_TAG_INDEX) {
			defaultClickRate = 0.032;
		}

		String user_location = extend2Map.getOrDefault("location", "");
		String user_location_id = extend2Map.getOrDefault("location_id", "");
		String user_area_id = getUserAreaId(request_area_id, user_location_id, user_minning_extra_area_id, "userUnknownAreaId");
		String user_city_tag = getUserCityTag(request_area_id, user_location, user_minning_city_tag, user_area_id, "userUnknownLocation");
		String user_province_tag = getProvinceTag(user_city_tag, user_minning_province_tag);

		int province_index = Configs.provinceConfigMap.getOrDefault(user_province_tag, 35);
		double mblog_province_group_ctr = defaultClickRate;
		double mblog_province_group_click = 0;
		double mblog_province_group_expo = 0;
		String[] provinceInfoArray = extend2Map.getOrDefault("province_info", "").split(",");
		if (province_index < 35 && provinceInfoArray.length == 70) {
			mblog_province_group_expo = parseValueDouble(provinceInfoArray[province_index]);
			mblog_province_group_click = parseValueDouble(provinceInfoArray[province_index + 35]);
			mblog_province_group_ctr = getClickRate(mblog_province_group_expo, mblog_province_group_click, defaultClickRate);
		}

		JsonObject exposure_info = getJsonObject(getJsonString(extend_info, "exposure_interaction_rate", "{}"));
		String mblog_total_read_num = getJsonString(extend_info, "total_read_num", "0");
		int mblog_interact_num = getJsonInt(exposure_info, "inter_act_num", 0);
		String mblog_inter_act_num_recent = getJsonString(exposure_info, "inter_act_num_recent", "0");
		double mblog_hot_ret_num = getJsonDouble(exposure_info, "hot_ret_num", 0);
		double mblog_hot_cmt_num = getJsonDouble(exposure_info, "hot_cmt_num", 0);
		double mblog_hot_like_num = getJsonDouble(exposure_info, "hot_like_num", 0);
		String mblog_hot_ret_num_recent = getJsonString(exposure_info, "hot_ret_num_recent", "0");
		String mblog_hot_cmt_num_recent = getJsonString(exposure_info, "hot_cmt_num_recent", "0");
		String mblog_hot_like_num_recent = getJsonString(exposure_info, "hot_like_num_recent", "0");
		String time_born_gender_action_list = getJsonArray(exposure_info, "time_born_gender_action_list", new JsonArray()).toString();

		user_born = getExtend2Value(user_born, extend2Map.getOrDefault("born", ""), "unk_born");
		int user_born_index = getBornIndex(user_born);
		user_gender = getExtend2Value(user_gender, extend2Map.getOrDefault("gender", ""), "no_gender");
		int user_gender_index = getGenderIndex(user_gender);
		String[] time_born_genders = time_born_gender_action_list.trim().replaceAll("^[\\[|\\]| ]+", "").replaceAll("[\\[|\\]| ]+$", "").split(",");
		if (time_born_genders.length != 180) {
			return;
		}
		int all_group_index = user_born_index * 3 * 6 + user_gender_index * 6;
		int hour_group_index = all_group_index + 5 * 3 * 6;
		String[] allGroupMsgArray = Arrays.copyOfRange(time_born_genders, all_group_index, all_group_index + 6);
		String[] hourGroupMsgArray = Arrays.copyOfRange(time_born_genders, hour_group_index, hour_group_index + 6);

		double mblog_group_expo_num = parseValueDouble(allGroupMsgArray[0]);
		double mblog_group_act_num = parseValueDouble(allGroupMsgArray[1]);
		double mblog_group_interact_num = parseValueDouble(allGroupMsgArray[2]);
		double mblog_group_ret_num = parseValueDouble(allGroupMsgArray[3]);
		double mblog_group_cmt_num = parseValueDouble(allGroupMsgArray[4]);
		double mblog_group_like_num = parseValueDouble(allGroupMsgArray[5]);

		double mblog_group_expo_recent_num = parseValueDouble(hourGroupMsgArray[0]);
		double mblog_group_act_recent_num = parseValueDouble(hourGroupMsgArray[1]);
		double mblog_group_interact_recent_num = parseValueDouble(hourGroupMsgArray[2]);
		double mblog_group_ret_recent_num = parseValueDouble(hourGroupMsgArray[3]);
		double mblog_group_cmt_recent_num = parseValueDouble(hourGroupMsgArray[4]);
		double mblog_group_like_recent_num = parseValueDouble(hourGroupMsgArray[5]);

		String is_match_long_interest = extend2Map.getOrDefault("match_long_inte", "0");
		String is_match_short_interest = extend2Map.getOrDefault("match_short_inte", "0");
		String is_match_near_interest = extend2Map.getOrDefault("match_near_inte", "0");
		String is_match_instant_interest = extend2Map.getOrDefault("match_instant_inte", "0");

		exposure_position = updateExposurePosition(exposure_position, extend2Map.getOrDefault("rank", ""));

		user_frequency = getExtend2Value(user_frequency, extend2Map.getOrDefault("login_freq", ""));
		String user_active_type = extend2Map.getOrDefault("active_type", "-1");

		double mblog_real_expo_num = parseValueDouble(extend2Map.getOrDefault("real_expo_num", "-1"));
		mblog_real_expo_num = (mblog_real_expo_num > 0) ? mblog_real_expo_num : mblog_expose_num;

		// time_born_gender_real_read: 70_male,70_female,70_other,80_male,80_female,80_other,90_male,90_female,90_other,00_male,00_female,00_other,other_male,other_female,other_other
		String mblogTimeBornGenderRealRead = extend2Map.getOrDefault("time_born_gender_real_read", "");
		double mblog_real_group_expo_num = getBornGenderRealExpoNum(user_born, user_gender, mblogTimeBornGenderRealRead, mblog_real_expo_num);

		double mblog_click_rate = getClickRate(mblog_expose_num, mblog_act_num, defaultClickRate);
		double mblog_interact_rate = getInteractRate(mblog_expose_num, mblog_interact_num, defaultInteractRate);
		double mblog_group_click_rate = getClickRate(mblog_group_expo_num, mblog_group_act_num, mblog_click_rate);
		double mblog_group_interact_rate = getInteractRate(mblog_group_expo_num, mblog_group_interact_num, mblog_interact_rate);

		double mblog_real_click_rate = getWilsonValue(mblog_real_expo_num, mblog_act_num, defaultRealClickRate);
		double mblog_real_interact_rate = getWilsonValue(mblog_real_expo_num, mblog_interact_num, defaultRealInteractRate);
		double mblog_real_group_click_rate = getWilsonValue(mblog_real_group_expo_num, mblog_group_act_num, mblog_real_click_rate);
		double mblog_real_group_interact_rate = getWilsonValue(mblog_real_group_expo_num, mblog_group_interact_num, mblog_real_interact_rate);

		double mblog_real_read_duration = parseValueDouble(extend2Map.getOrDefault("real_read_duration", "-1"));
		double mblog_real_read_uv = parseValueDouble(extend2Map.getOrDefault("real_read_uv", "-1"));
		double mblog_read_duration_avg = (mblog_real_read_duration > 0 && mblog_real_read_uv > 0) ? (mblog_real_read_duration / mblog_real_read_uv) : 0;

		List<Double> mblogCityLevelInfoList = new ArrayList<>();
		mblogCityLevelInfoList.add(mblog_real_expo_num);
		mblogCityLevelInfoList.add(0.0);
		mblogCityLevelInfoList.add(0.0);
		String user_city_level = getUserCityLevel(user_city_tag, user_minning_city_level);
		getCityLevelInfo(user_city_level, extend2Map.getOrDefault("city_level_real_read_info", ""), mblogCityLevelInfoList);
		double mblog_real_city_level_expo_num = mblogCityLevelInfoList.get(0);
		double mblog_real_city_level_act_num = mblogCityLevelInfoList.get(1);
		double mblog_real_city_level_interact_num = mblogCityLevelInfoList.get(2);

		double mblog_real_city_level_click_rate = getWilsonValue(mblog_real_city_level_expo_num, mblog_real_city_level_act_num, mblog_real_click_rate);
		double mblog_real_city_level_interact_rate = getWilsonValue(mblog_real_city_level_expo_num, mblog_real_city_level_interact_num, mblog_real_interact_rate);

		// NOTE: data error, need accumulate
//		mblogProvinceRealReadInfo = extend2Map.getOrDefault("province_real_read_info", "-1");

		String[] newClickNumArray = extend2Map.getOrDefault("new_clicknum", "").split(",");
		if (newClickNumArray.length != 8) {
			return;
		}
		double mblog_click_pic_num = parseValueDouble(newClickNumArray[0]);
		double mblog_click_video_num = parseValueDouble(newClickNumArray[1]);
		double mblog_click_single_page_num = parseValueDouble(newClickNumArray[2]);
		double mblog_click_follow_num = parseValueDouble(newClickNumArray[3]);
		double mblog_click_article_num = parseValueDouble(newClickNumArray[4]);
		mblog_hot_ret_num = Math.max(parseValueDouble(newClickNumArray[5]), mblog_hot_ret_num);
		mblog_hot_cmt_num = Math.max(parseValueDouble(newClickNumArray[6]), mblog_hot_cmt_num);
		mblog_hot_like_num = Math.max(parseValueDouble(newClickNumArray[7]), mblog_hot_like_num);
		double mblog_new_click_num = mblog_click_pic_num + mblog_click_video_num + mblog_click_single_page_num
				+ mblog_click_follow_num + mblog_click_article_num + mblog_hot_ret_num
				+ mblog_hot_cmt_num + mblog_hot_like_num;

		double midClickNumRateDefault = 0.1;
		double mblog_click_num_rate = genClickNumRate(mblog_expose_num, mblog_new_click_num, midClickNumRateDefault);
		double mblog_group_click_num_rate = mblog_group_click_rate / mblog_click_rate * mblog_click_num_rate;

		double mblog_click_pic_rate = getClickRate(mblog_expose_num, mblog_click_pic_num, defaultClickRate);
		double mblog_click_video_rate = getClickRate(mblog_expose_num, mblog_click_video_num, defaultClickRate);
		double mblog_click_single_page_rate = getClickRate(mblog_expose_num, mblog_click_single_page_num, defaultClickRate);
		double mblog_click_follow_rate = 1000 * getClickRate(mblog_expose_num, mblog_click_follow_num, defaultClickRate);
		double mblog_click_article_rate = getClickRate(mblog_expose_num, mblog_click_article_num, defaultClickRate);
		double mblog_hot_ret_rate = 1000 * getClickRate(mblog_expose_num, mblog_hot_ret_num, defaultClickRate);
		double mblog_hot_cmt_rate = 1000 * getClickRate(mblog_expose_num, mblog_hot_cmt_num, defaultClickRate);
		double mblog_hot_like_rate = getClickRate(mblog_expose_num, mblog_hot_like_num, defaultClickRate);

		double mblog_real_click_pic_rate = getWilsonValue(mblog_real_expo_num, mblog_click_pic_num, defaultClickRate);
		double mblog_real_click_video_rate = getWilsonValue(mblog_real_expo_num, mblog_click_video_num, defaultClickRate);
		double mblog_real_click_sing_page_rate = getWilsonValue(mblog_real_expo_num, mblog_click_single_page_num, defaultClickRate);
		double mblog_real_click_follow_rate = getWilsonValue(mblog_real_expo_num, mblog_click_follow_num, defaultClickRate);
		double mblog_real_click_article_rate = getWilsonValue(mblog_real_expo_num, mblog_click_article_num, defaultClickRate);
		double mblog_real_ret_rate = getWilsonValue(mblog_real_expo_num, mblog_hot_ret_num, defaultClickRate);
		double mblog_real_cmt_rate = getWilsonValue(mblog_real_expo_num, mblog_hot_cmt_num, defaultClickRate);
		double mblog_real_like_rate = getWilsonValue(mblog_real_expo_num, mblog_hot_like_num, defaultClickRate);

		double mblog_group_click_pic_rate = mblog_group_click_rate / mblog_click_rate * mblog_click_pic_rate;
		double mblog_group_click_video_rate = mblog_group_click_rate / mblog_click_rate * mblog_click_video_rate;
		double mblog_group_click_single_page_rate = mblog_group_click_rate / mblog_click_rate * mblog_click_single_page_rate;
		double mblog_group_click_follow_rate = mblog_group_click_rate / mblog_click_rate * mblog_click_follow_rate;
		double mblog_group_click_article_rate = mblog_group_click_rate / mblog_click_rate * mblog_click_article_rate;

		double mblog_real_group_click_pic_rate = mblog_real_group_click_rate / mblog_real_click_rate * mblog_real_click_pic_rate;
		double mblog_real_group_click_video_rate = mblog_real_group_click_rate / mblog_real_click_rate * mblog_real_click_video_rate;
		double mblog_real_group_click_single_page_rate = mblog_real_group_click_rate / mblog_real_click_rate * mblog_real_click_sing_page_rate;
		double mblog_real_group_click_follow_rate = mblog_real_group_click_rate / mblog_real_click_rate * mblog_real_click_follow_rate;
		double mblog_real_group_click_article_rate = mblog_real_group_click_rate / mblog_real_click_rate * mblog_real_click_article_rate;

		double mblog_click_pic_num_norm = getNormValue(mblog_click_pic_num, -0.000003);
		double mblog_click_video_num_norm = getNormValue(mblog_click_video_num, -0.000008);
		double mblog_click_single_page_num_norm = getNormValue(mblog_click_single_page_num, -0.000011);
		double mblog_click_follow_num_norm = getNormValue(mblog_click_follow_num, -0.004);
		double mblog_click_article_num_norm = getNormValue(mblog_click_article_num, -0.0002);

		double mblog_hot_ret_num_norm = getNormValue(mblog_hot_ret_num, -0.004);
		double mblog_hot_cmt_num_norm = getNormValue(mblog_hot_cmt_num, -0.1);
		double mblog_hot_like_num_norm = getNormValue(mblog_hot_like_num, -0.0002);

		double mblog_ret_num_norm = getNormValue(mblog_ret_num, -0.001);
		double mblog_cmt_num_norm = getNormValue(mblog_cmt_num, -0.00095);
		double mblog_like_num_norm = getNormValue(mblog_like_num, -0.00012);

		double mblog_hot_heat = 0.6 * mblog_hot_ret_num + 0.4 * mblog_hot_like_num;
		double mblog_hot_heat_norm = getNormValue(mblog_ret_num, -0.001);

		double mblog_heat = 0.6 * mblog_ret_num + 0.4 * mblog_like_num;
		double mblog_heat_norm = getNormValue(mblog_ret_num, -0.0005);

		Map<String, Double> mblogFirstTagMap = new HashMap<>();
		Map<String, Double> mblogSecondTagMap = new HashMap<>();
		Map<String, Double> mblogThirdTagMap = new HashMap<>();
		Map<String, Double> mblogKeywordsTagMap = new HashMap<>();
		Map<String, Double> mblogTopicTagMap = new HashMap<>();
		Map<String, Double> mblogAreaTagMap = new HashMap<>();
		Map<String, Tag> firstDocTags = new HashMap<>();
		Map<String, Tag> secondDocTags = new HashMap<>();
		Map<String, Tag> thirdDocTags = new HashMap<>();
		int is_match_location = 0;
		try {
			List<Map> docTags = jsonToList(content_tag);
			for (Map current_map : docTags) {
				int tagType = parseValueInteger(current_map.get("type"));
				String tagCategory = parseValueString(current_map.get("category"));
				String current_tag_id = parseValueString(current_map.get("tagid"));
				double current_rel_weight = parseValueDouble(current_map.get("rel_weight"));
				Tag t = new Tag();
				t.weight = current_rel_weight;
				t.type = tagType;
				t.category = tagCategory;
				if (tagType == 1) {
					mblogFirstTagMap.put(current_tag_id, current_rel_weight);
					firstDocTags.put(current_tag_id, t);
				} else if (tagType == 2) {
					mblogSecondTagMap.put(current_tag_id, current_rel_weight);
					secondDocTags.put(current_tag_id, t);
				} else if (tagType == 3) {
					if (current_tag_id.startsWith("1042015:tagTopic_")) {
						mblogTopicTagMap.put(current_tag_id, current_rel_weight);
					} else if (current_tag_id.startsWith("1042015:keyWord_")) {
						mblogKeywordsTagMap.put(current_tag_id, current_rel_weight);
					} else {
						mblogThirdTagMap.put(current_tag_id, current_rel_weight);
						thirdDocTags.put(current_tag_id, t);
					}
				} else if (tagType == 5
						|| current_tag_id.startsWith("1042015:province_")
						|| current_tag_id.startsWith("1042015:city_")
						|| current_tag_id.startsWith("80")) {
					String cur_city_tag = areaId2CityTag(current_tag_id, "mblogUnknownLocation");
					if (is_match_location <= 1) {
						is_match_location = 1;
						if (cur_city_tag.equals(user_city_tag)) {
							is_match_location = 3;
						} else if (getProvinceTag(cur_city_tag).equals(user_province_tag)) {
							is_match_location = 2;
						}
					} else if (2 == is_match_location && cur_city_tag.contains(user_city_tag)) {
						is_match_location = 3;
					}
					if (current_tag_id.startsWith("1042015:province_")
							|| current_tag_id.startsWith("1042015:city_")
							|| current_tag_id.startsWith("80")) {
						mblogAreaTagMap.put(current_tag_id, current_rel_weight);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		String mblog_first_max_tag = getMaxValueTag(mblogFirstTagMap, 0);
		String mblog_second_max_tag = getMaxValueTag(mblogSecondTagMap, 0.6);
		String mblog_third_max_tag = getMaxValueTag(mblogThirdTagMap, 0.6);
		String mblog_topic_max_tag = getMaxValueTag(mblogTopicTagMap, 0.6);
		String mblog_keyword_max_tag = getMaxValueTag(mblogKeywordsTagMap, 0.6);

		String mblog_first_tags = map2StringDoubleSequence(mblogFirstTagMap);
		String mblog_second_tags = map2StringDoubleSequence(mblogSecondTagMap);
		String mblog_third_tags = map2StringDoubleSequence(mblogThirdTagMap);
		String mblog_topic_tags = map2StringDoubleSequence(mblogTopicTagMap);
		String mblog_keyword_tags = map2StringDoubleSequence(mblogKeywordsTagMap);
		String mblog_area_tags = map2StringDoubleSequence(mblogAreaTagMap);

		// --- user interest cold start
		Map<String, Double> user_interest_cold_start_map = new HashMap<>();
		try {
			String[] user_inter_cold_start_str = user_interest_cold_start.get(user_gender + " " + user_born).split("\t");
			for (String current_tag : user_inter_cold_start_str) {
				String[] cate_weight = current_tag.split("@");
				if (cate_weight.length == 2) {
					String c_name = cate_weight[0].replace("vertical_t_", "");
					double c_weight = parseValueDouble(cate_weight[1]);
					if (!isEmptyString(c_name) && c_weight > 0 && checkIsValidFirstTag(c_name)) {
						user_interest_cold_start_map.put(c_name, c_weight);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		String user_cold_start_tags = map2StringDoubleSequence(user_interest_cold_start_map);

		// --- user long interest
		Map<String, Double> userLongFirstMap = new HashMap<>();
		Map<String, Double> userLongSecondMap = new HashMap<>();
		Map<String, Double> userLongThirdMap = new HashMap<>();
		try {
			String[] userLongTags = user_long_interests.split(",", -1);
			String firstTags = userLongTags[0];
			String secondTags = userLongTags[1];
			String thirdTags = userLongTags[2];
			if (firstTags.length() > 0) {
				for (String tagWeight : firstTags.split("\\|")) {
					String[] tagWeightArray = tagWeight.split("@");
					if (tagWeightArray.length == 2) {
						String c_name = tagWeightArray[0].replace("vertical_t_", "");
						double c_weight = parseValueDouble(tagWeightArray[1]);
						if (!isEmptyString(c_name) && c_weight > 0 && checkIsValidFirstTag(c_name)) {
							userLongFirstMap.put(c_name, c_weight);
						}
					}
				}
			}
			if (secondTags.length() > 0) {
				for (String tagWeight : secondTags.split("\\|")) {
					String[] tagWeightArray = tagWeight.split("@");
					if (tagWeightArray.length == 2) {
						String c_name = tagWeightArray[0].replace("vertical_t_", "");
						double c_weight = parseValueDouble(tagWeightArray[1]);
						if (!isEmptyString(c_name) && c_weight > 0 && checkIsValidSecondTag(c_name)) {
							userLongSecondMap.put(c_name, c_weight);
						}
					}
				}
			}
			if (thirdTags.length() > 0) {
				for (String tagWeight : thirdTags.split("\\|")) {
					String[] tagWeightArray = tagWeight.split("@");
					if (tagWeightArray.length == 2) {
						String c_name = tagWeightArray[0].replace("vertical_t_", "");
						double c_weight = parseValueDouble(tagWeightArray[1]);
						if (!isEmptyString(c_name) && c_weight > 0 && checkIsValidThirdTag(c_name)) {
							userLongThirdMap.put(c_name, c_weight);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		String user_long_first_tags = map2StringDoubleSequence(userLongFirstMap);
		String user_long_second_tags = map2StringDoubleSequence(userLongSecondMap);
		String user_long_third_tags = map2StringDoubleSequence(userLongThirdMap);

		// --- user short interest
		Map<String, Double> userShortFirstMap = new HashMap<>();
		Map<String, Double> userShortSecondMap = new HashMap<>();
		Map<String, Double> userShortThirdMap = new HashMap<>();
		try {
			String[] userShortTags = user_short_interests.split(",", -1);
			String firstTags = userShortTags[0];
			String secondTags = userShortTags[1];
			String thirdTags = userShortTags[2];
			if (firstTags.length() > 0) {
				for (String tagWeight : firstTags.split("\\|")) {
					String[] tagWeightArray = tagWeight.split("@");
					if (tagWeightArray.length == 2) {
						String c_name = tagWeightArray[0].replace("vertical_t_", "");
						double c_weight = parseValueDouble(tagWeightArray[1]);
						if (!isEmptyString(c_name) && c_weight > 0 && checkIsValidFirstTag(c_name)) {
							userShortFirstMap.put(c_name, c_weight);
						}
					}
				}
			}
			if (secondTags.length() > 0) {
				for (String tagWeight : secondTags.split("\\|")) {
					String[] tagWeightArray = tagWeight.split("@");
					if (tagWeightArray.length == 2) {
						String c_name = tagWeightArray[0].replace("vertical_t_", "");
						double c_weight = parseValueDouble(tagWeightArray[1]);
						if (!isEmptyString(c_name) && c_weight > 0 && checkIsValidSecondTag(c_name)) {
							userShortSecondMap.put(c_name, c_weight);
						}
					}
				}
			}
			if (thirdTags.length() > 0) {
				for (String tagWeight : thirdTags.split("\\|")) {
					String[] tagWeightArray = tagWeight.split("@");
					if (tagWeightArray.length == 2) {
						String c_name = tagWeightArray[0].replace("vertical_t_", "");
						double c_weight = parseValueDouble(tagWeightArray[1]);
						if (!isEmptyString(c_name) && c_weight > 0 && checkIsValidThirdTag(c_name)) {
							userShortThirdMap.put(c_name, c_weight);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		String user_short_first_tags = map2StringDoubleSequence(userShortFirstMap);
		String user_short_second_tags = map2StringDoubleSequence(userShortSecondMap);
		String user_short_third_tags = map2StringDoubleSequence(userShortThirdMap);
		String match_user_author_intimacy = ParseIntimacy(user_intimacy, author_id);

		// 用户兴趣标签
		Map<String, Double> userMergedLongFirstTagMap = new HashMap<>();
		Map<String, Double> userMergedFirstTagMap = getUserInterestFirst(user_interest_cold_start_map, userLongFirstMap, userShortFirstMap, userMergedLongFirstTagMap);
		Map<String, Double> userMergedSecondTagMap = getUserInterestSecondAndThird(userLongSecondMap, userShortSecondMap);
		Map<String, Double> userMergedThirdTagMap = getUserInterestSecondAndThird(userLongThirdMap, userShortThirdMap);

		String user_merged_first_tags = map2StringDoubleSequence(userMergedFirstTagMap);
		String user_merged_second_tags = map2StringDoubleSequence(userMergedSecondTagMap);
		String user_merged_third_tags = map2StringDoubleSequence(userMergedThirdTagMap);

		if (userMergedFirstTagMap.isEmpty() || mblogFirstTagMap.isEmpty()) {
			return;
		}

		String match_first_tag = getMaxMatchTag(userMergedFirstTagMap, firstDocTags, recall_category, 1);
		String match_second_tag = getMaxMatchTag(userMergedSecondTagMap, secondDocTags, recall_category, 2);
		String match_third_tag = getMaxMatchTag(userMergedThirdTagMap, thirdDocTags, recall_category, 3);

		String match_first_tag_v2 = getMaxMatchTagV2(userMergedFirstTagMap, mblogFirstTagMap);
		String match_second_tag_v2 = getMaxMatchTagV2(userMergedSecondTagMap, mblogSecondTagMap);
		String match_third_tag_v2 = getMaxMatchTagV2(userMergedThirdTagMap, mblogThirdTagMap);

		String match_first_long_tag = getMaxMatchTagV2(userMergedLongFirstTagMap, mblogFirstTagMap);
		String match_second_long_tag = getMaxMatchTagV2(userLongSecondMap, mblogSecondTagMap);
		String match_third_long_tag = getMaxMatchTagV2(userLongThirdMap, mblogThirdTagMap);

		String match_first_short_tag = getMaxMatchTagV2(userShortFirstMap, mblogFirstTagMap);
		String match_second_short_tag = getMaxMatchTagV2(userShortSecondMap, mblogSecondTagMap);
		String match_third_short_tag = getMaxMatchTagV2(userShortThirdMap, mblogThirdTagMap);

		double match_first_tag_user_value = (!match_first_tag.equals("invalidValue")) ? userMergedFirstTagMap.getOrDefault(match_first_tag, 0.0) : 0;
		double match_second_tag_user_value = (!match_second_tag.equals("invalidValue")) ? userMergedSecondTagMap.getOrDefault(match_second_tag, 0.0) : 0;
		double match_third_tag_user_Value = (!match_third_tag.equals("invalidValue")) ? userMergedThirdTagMap.getOrDefault(match_third_tag, 0.0) : 0;
		double match_first_tag_mblog_value = (!match_first_tag.equals("invalidValue") && mblogFirstTagMap.containsKey(match_first_tag)) ? mblogFirstTagMap.get(match_first_tag) : 0;
		double match_second_tag_mblog_value = (!match_second_tag.equals("invalidValue") && mblogSecondTagMap.containsKey(match_second_tag)) ? mblogSecondTagMap.get(match_second_tag) : 0;
		double match_third_tag_mblog_value = (!match_third_tag.equals("invalidValue") && mblogThirdTagMap.containsKey(match_third_tag)) ? mblogThirdTagMap.get(match_third_tag) : 0;

		double match_first_tag_user_value_v2 = (!match_first_tag_v2.equals("invalidValue")) ? userMergedFirstTagMap.getOrDefault(match_first_tag_v2, 0.0) : 0;
		double match_second_tag_user_value_v2 = (!match_second_tag_v2.equals("invalidValue")) ? userMergedSecondTagMap.getOrDefault(match_second_tag_v2, 0.0) : 0;
		double match_third_tag_user_Value_v2 = (!match_third_tag_v2.equals("invalidValue")) ? userMergedThirdTagMap.getOrDefault(match_third_tag_v2, 0.0) : 0;
		double match_first_tag_mblog_value_v2 = (!match_first_tag_v2.equals("invalidValue") && mblogFirstTagMap.containsKey(match_first_tag_v2)) ? mblogFirstTagMap.get(match_first_tag_v2) : 0;
		double match_second_tag_mblog_value_v2 = (!match_second_tag_v2.equals("invalidValue") && mblogSecondTagMap.containsKey(match_second_tag_v2)) ? mblogSecondTagMap.get(match_second_tag_v2) : 0;
		double match_third_tag_mblog_value_v2 = (!match_third_tag_v2.equals("invalidValue") && mblogThirdTagMap.containsKey(match_third_tag_v2)) ? mblogThirdTagMap.get(match_third_tag_v2) : 0;

		double match_first_tag_user_long_value = (!match_first_long_tag.equals("invalidValue")) ? userMergedFirstTagMap.getOrDefault(match_first_long_tag, 0.0) : 0;
		double match_second_tag_user_long_value = (!match_second_long_tag.equals("invalidValue")) ? userMergedSecondTagMap.getOrDefault(match_second_long_tag, 0.0) : 0;
		double match_third_tag_user_long_Value = (!match_third_long_tag.equals("invalidValue")) ? userMergedThirdTagMap.getOrDefault(match_third_long_tag, 0.0) : 0;
		double match_first_tag_mblog_long_value = (!match_first_long_tag.equals("invalidValue") && mblogFirstTagMap.containsKey(match_first_long_tag)) ? mblogFirstTagMap.get(match_first_long_tag) : 0;
		double match_second_tag_mblog_long_value = (!match_second_long_tag.equals("invalidValue") && mblogSecondTagMap.containsKey(match_second_long_tag)) ? mblogSecondTagMap.get(match_second_long_tag) : 0;
		double match_third_tag_mblog_long_value = (!match_third_long_tag.equals("invalidValue") && mblogThirdTagMap.containsKey(match_third_long_tag)) ? mblogThirdTagMap.get(match_third_long_tag) : 0;

		double match_first_tag_user_short_value = (!match_first_short_tag.equals("invalidValue")) ? userMergedFirstTagMap.getOrDefault(match_first_short_tag, 0.0) : 0;
		double match_second_tag_user_short_value = (!match_second_short_tag.equals("invalidValue")) ? userMergedSecondTagMap.getOrDefault(match_second_short_tag, 0.0) : 0;
		double match_third_tag_user_short_Value = (!match_third_short_tag.equals("invalidValue")) ? userMergedThirdTagMap.getOrDefault(match_third_short_tag, 0.0) : 0;
		double match_first_tag_mblog_short_value = (!match_first_short_tag.equals("invalidValue") && mblogFirstTagMap.containsKey(match_first_short_tag)) ? mblogFirstTagMap.get(match_first_short_tag) : 0;
		double match_second_tag_mblog_short_value = (!match_second_short_tag.equals("invalidValue") && mblogSecondTagMap.containsKey(match_second_short_tag)) ? mblogSecondTagMap.get(match_second_short_tag) : 0;
		double match_third_tag_mblog_short_value = (!match_third_short_tag.equals("invalidValue") && mblogThirdTagMap.containsKey(match_third_short_tag)) ? mblogThirdTagMap.get(match_third_short_tag) : 0;

		String firstLevelMatchTagKey = "tag_ctr,".concat(match_first_tag);
		String secondLevelMatchTagKey = "tag_ctr,".concat(match_second_tag);
		String thirdLevelMatchTagKey = "tag_ctr,".concat(match_third_tag);
		double match_first_group_ctr = defaultClickRate;
		double match_second_group_ctr = defaultClickRate;
		double match_third_group_ctr = defaultClickRate;
		if (extend2Map.containsKey(firstLevelMatchTagKey)) {
			String[] tagCtr = extend2Map.get(firstLevelMatchTagKey).split(",");
			match_first_group_ctr = getClickRate(parseValueDouble(tagCtr[0]), parseValueDouble(tagCtr[1]), mblog_click_rate);
		} else if (!"invalidValue".equals(mblog_first_max_tag) && extend2Map.containsKey("tag_ctr,".concat(mblog_first_max_tag))) {
			firstLevelMatchTagKey = "tag_ctr,".concat(mblog_first_max_tag);
			String[] tagCtr = extend2Map.get(firstLevelMatchTagKey).split(",");
			match_first_group_ctr = getClickRate(parseValueDouble(tagCtr[0]), parseValueDouble(tagCtr[1]), mblog_click_rate);
		}

		if (extend2Map.containsKey(secondLevelMatchTagKey)) {
			String[] tagCtr = extend2Map.get(secondLevelMatchTagKey).split(",");
			match_second_group_ctr = getClickRate(parseValueDouble(tagCtr[0]), parseValueDouble(tagCtr[1]), mblog_click_rate);
		} else if (!"invalidValue".equals(mblog_second_max_tag) && extend2Map.containsKey("tag_ctr,".concat(mblog_second_max_tag))) {
			secondLevelMatchTagKey = "tag_ctr,".concat(mblog_second_max_tag);
			String[] tagCtr = extend2Map.get(secondLevelMatchTagKey).split(",");
			match_second_group_ctr = getClickRate(parseValueDouble(tagCtr[0]), parseValueDouble(tagCtr[1]), mblog_click_rate);
		}

		if (extend2Map.containsKey(thirdLevelMatchTagKey)) {
			String[] tagCtr = extend2Map.get(thirdLevelMatchTagKey).split(",");
			match_third_group_ctr = getClickRate(parseValueDouble(tagCtr[0]), parseValueDouble(tagCtr[1]), mblog_click_rate);
		} else if (!"invalidValue".equals(mblog_third_max_tag) && extend2Map.containsKey("tag_ctr,".concat(mblog_third_max_tag))) {
			thirdLevelMatchTagKey = "tag_ctr,".concat(mblog_third_max_tag);
			String[] tagCtr = extend2Map.get(thirdLevelMatchTagKey).split(",");
			match_third_group_ctr = getClickRate(parseValueDouble(tagCtr[0]), parseValueDouble(tagCtr[1]), mblog_click_rate);
		}


		String firstLevelMatchTagKeyV2 = "tag_ctr,".concat(match_first_tag_v2);
		String secondLevelMatchTagKeyV2 = "tag_ctr,".concat(match_second_tag_v2);
		String thirdLevelMatchTagKeyV2 = "tag_ctr,".concat(match_third_tag_v2);
		double match_first_group_v2_ctr = defaultClickRate;
		double match_second_group_v2_ctr = defaultClickRate;
		double match_third_group_v2_ctr = defaultClickRate;
		if (extend2Map.containsKey(firstLevelMatchTagKeyV2)) {
			String[] tagCtr = extend2Map.get(firstLevelMatchTagKeyV2).split(",");
			match_first_group_v2_ctr = getWilsonValue(parseValueDouble(tagCtr[0]), parseValueDouble(tagCtr[1]), mblog_click_rate);
		}
		if (extend2Map.containsKey(secondLevelMatchTagKeyV2)) {
			String[] tagCtr = extend2Map.get(secondLevelMatchTagKeyV2).split(",");
			match_second_group_v2_ctr = getWilsonValue(parseValueDouble(tagCtr[0]), parseValueDouble(tagCtr[1]), match_first_group_v2_ctr);
		}
		if (extend2Map.containsKey(thirdLevelMatchTagKeyV2)) {
			String[] tagCtr = extend2Map.get(thirdLevelMatchTagKeyV2).split(",");
			match_third_group_v2_ctr = getWilsonValue(parseValueDouble(tagCtr[0]), parseValueDouble(tagCtr[1]), match_second_group_v2_ctr);
		}

		String firstLevelMatchTagKeyLong = "tag_ctr,".concat(match_first_long_tag);
		String secondLevelMatchTagKeyLong = "tag_ctr,".concat(match_second_long_tag);
		String thirdLevelMatchTagKeyLong = "tag_ctr,".concat(match_third_long_tag);
		double match_first_group_long_ctr = defaultClickRate;
		double match_second_group_long_ctr = defaultClickRate;
		double match_third_group_long_ctr = defaultClickRate;
		if (extend2Map.containsKey(firstLevelMatchTagKeyLong)) {
			String[] tagCtr = extend2Map.get(firstLevelMatchTagKeyLong).split(",");
			match_first_group_long_ctr = getWilsonValue(parseValueDouble(tagCtr[0]), parseValueDouble(tagCtr[1]), mblog_click_rate);
		}
		if (extend2Map.containsKey(secondLevelMatchTagKeyLong)) {
			String[] tagCtr = extend2Map.get(secondLevelMatchTagKeyLong).split(",");
			match_second_group_long_ctr = getWilsonValue(parseValueDouble(tagCtr[0]), parseValueDouble(tagCtr[1]), match_first_group_long_ctr);
		}
		if (extend2Map.containsKey(thirdLevelMatchTagKeyLong)) {
			String[] tagCtr = extend2Map.get(thirdLevelMatchTagKeyLong).split(",");
			match_third_group_long_ctr = getWilsonValue(parseValueDouble(tagCtr[0]), parseValueDouble(tagCtr[1]), match_second_group_long_ctr);
		}

		String firstLevelMatchTagKeyShort = "tag_ctr,".concat(match_first_tag_v2);
		String secondLevelMatchTagKeyShort = "tag_ctr,".concat(match_second_tag_v2);
		String thirdLevelMatchTagKeyShort = "tag_ctr,".concat(match_third_tag_v2);
		double match_first_group_short_ctr = defaultClickRate;
		double match_second_group_short_ctr = defaultClickRate;
		double match_third_group_short_ctr = defaultClickRate;
		if (extend2Map.containsKey(firstLevelMatchTagKeyShort)) {
			String[] tagCtr = extend2Map.get(firstLevelMatchTagKeyShort).split(",");
			match_first_group_short_ctr = getWilsonValue(parseValueDouble(tagCtr[0]), parseValueDouble(tagCtr[1]), mblog_click_rate);
		}
		if (extend2Map.containsKey(secondLevelMatchTagKeyShort)) {
			String[] tagCtr = extend2Map.get(secondLevelMatchTagKeyShort).split(",");
			match_second_group_short_ctr = getWilsonValue(parseValueDouble(tagCtr[0]), parseValueDouble(tagCtr[1]), match_first_group_short_ctr);
		}
		if (extend2Map.containsKey(thirdLevelMatchTagKeyShort)) {
			String[] tagCtr = extend2Map.get(thirdLevelMatchTagKeyShort).split(",");
			match_third_group_short_ctr = getWilsonValue(parseValueDouble(tagCtr[0]), parseValueDouble(tagCtr[1]), match_second_group_short_ctr);
		}

		Map<String, String> matchFirstTagCTRMap = new HashMap<>();
		Map<String, String> matchSecondTagCTRMap = new HashMap<>();
		Map<String, String> matchThirdTagCTRMap = new HashMap<>();
		// NOTE: ZhangBiao maybe lost some tags of doc
		for (Map.Entry<String, String> entry : extend2Map.entrySet()) {
			String cur_key = entry.getKey();
			if (cur_key.startsWith("tag_ctr")) {
				String cur_tagid = cur_key.substring(8);
				if (cur_tagid.contains("tagCategory")) {
					matchFirstTagCTRMap.put(cur_tagid, entry.getValue());
				} else if (cur_tagid.contains("abilityTag")) {
					matchSecondTagCTRMap.put(cur_tagid, entry.getValue());
				} else {
					matchThirdTagCTRMap.put(cur_tagid, entry.getValue());
				}
			}
		}
		String match_first_tag_ctrs = map2StringStringSequence(matchFirstTagCTRMap);
		String match_second_tag_ctrs = map2StringStringSequence(matchSecondTagCTRMap);
		String match_third_tag_ctrs = map2StringStringSequence(matchThirdTagCTRMap);

		String mblog_panorama_num = "0";
		String author_sunshine_credit = "0";
		try {
			Map<String, Object> dictionaryObjectMap = jsonToObjectMap(dictionary);
			mblog_panorama_num = parseValueString(dictionaryObjectMap.get("mblog_panorama_num"));
			mblog_panorama_num = isEmptyString(mblog_panorama_num) ? "0" : mblog_panorama_num;
			author_sunshine_credit = parseValueString(dictionaryObjectMap.get("user_sunshine_credit"));
			author_sunshine_credit = isEmptyString(author_sunshine_credit) ? "0" : author_sunshine_credit;
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		// 内容形式: 无, 视频, 短链, 长文, gif图, 长图, 全景图, 普通图
		int mblog_content_type = getContentFormType(
				mblog_miaopai_num,
				mblog_link_num,
				mblog_article_num,
				mblog_pic_num,
				mblog_gif_num,
				mblog_long_pic_num,
				parseValueInteger(mblog_panorama_num)
		);

		// 图片数
		int mblog_picture_num_index = getPictureNumIndex(mblog_pic_num);

		// 作者类型: 明星, 橙V, 蓝V, 其他
		int author_type_index = getAuthorVerifiedType(author_verified_type, author_property);

		int time_part = getTimePartIndex(expo_time);

		forward(
				// --- context
				is_click,
				actions,
				isautoplay,
				expo_time,
				String.valueOf(time_part),
				network_type,
				recommend_source,
				recall_category,
				String.valueOf(recall_category_id),
				real_duration,
				exposure_position,
				effect_weight,
				request_area_id,
				String.valueOf(province_index),

				v_valid_play_duration,
				v_object_duration,
				v_duration,
				v_replay_count,
				v_video_orientation,

				// --- user
				uid,
				user_frequency,
				user_active_type,
				user_born,
				user_gender,
				String.valueOf(user_born_index),
				String.valueOf(user_gender_index),

				user_minning_city_level,
				user_minning_extra_area_id,
				user_minning_city_name,
				user_minning_city_tag,
				user_minning_province_name,
				user_minning_province_tag,
				user_minning_city_weight,

				user_location,
				user_location_id,
				user_area_id,
				user_city_tag,
				user_province_tag,

				user_cold_start_tags,
				user_long_first_tags,
				user_long_second_tags,
				user_long_third_tags,
				user_short_first_tags,
				user_short_second_tags,
				user_short_third_tags,
				user_merged_first_tags,
				user_merged_second_tags,
				user_merged_third_tags,

				// --- author
				author_id,
				author_class,
				author_verified_type,
				author_property,
				String.valueOf(author_type_index),
				author_gender,
				author_city,
				author_province,
				author_followers_num,
				author_statuses_count,
				author_sunshine_credit,

				// --- mblog
				mid,
				mblog_text_len,
				mblog_level,
				mblog_topic_num,
				mblog_title_num,
				String.valueOf(mblog_miaopai_num),
				String.valueOf(mblog_link_num),
				String.valueOf(mblog_article_num),
				String.valueOf(mblog_pic_num),
				String.valueOf(mblog_gif_num),
				String.valueOf(mblog_long_pic_num),
				mblog_panorama_num,
				String.valueOf(mblog_content_type),
				String.valueOf(mblog_picture_num_index),

				String.valueOf(mblog_ret_num),
				String.valueOf(mblog_cmt_num),
				String.valueOf(mblog_like_num),
				mblog_ret_num_recent,
				mblog_cmt_num_recent,
				mblog_like_num_recent,
				String.valueOf(mblog_expose_num),
				String.valueOf(mblog_act_num),
				mblog_expose_num_recent,
				mblog_act_num_recent,
				mblog_article_read_num,
				mblog_miaopai_view_num,
				mblog_total_read_num,

				mblog_first_tags,
				mblog_second_tags,
				mblog_third_tags,
				mblog_topic_tags,
				mblog_keyword_tags,
				mblog_area_tags,

				mblog_first_max_tag,
				mblog_second_max_tag,
				mblog_third_max_tag,
				mblog_topic_max_tag,
				mblog_keyword_max_tag,

				String.valueOf(mblog_interact_num),
				mblog_inter_act_num_recent,
				String.valueOf(mblog_hot_ret_num),
				String.valueOf(mblog_hot_cmt_num),
				String.valueOf(mblog_hot_like_num),
				mblog_hot_ret_num_recent,
				mblog_hot_cmt_num_recent,
				mblog_hot_like_num_recent,

				String.valueOf(mblog_group_expo_num),
				String.valueOf(mblog_group_act_num),
				String.valueOf(mblog_group_interact_num),
				String.valueOf(mblog_group_ret_num),
				String.valueOf(mblog_group_cmt_num),
				String.valueOf(mblog_group_like_num),

				String.valueOf(mblog_group_expo_recent_num),
				String.valueOf(mblog_group_act_recent_num),
				String.valueOf(mblog_group_interact_recent_num),
				String.valueOf(mblog_group_ret_recent_num),
				String.valueOf(mblog_group_cmt_recent_num),
				String.valueOf(mblog_group_like_recent_num),

				String.valueOf(mblog_click_rate),
				String.valueOf(mblog_interact_rate),
				String.valueOf(mblog_group_click_rate),
				String.valueOf(mblog_group_interact_rate),

				String.valueOf(mblog_click_pic_num_norm),
				String.valueOf(mblog_click_video_num_norm),
				String.valueOf(mblog_click_single_page_num_norm),
				String.valueOf(mblog_click_follow_num_norm),
				String.valueOf(mblog_click_article_num_norm),
				String.valueOf(mblog_hot_ret_num_norm),
				String.valueOf(mblog_hot_cmt_num_norm),
				String.valueOf(mblog_hot_like_num_norm),
				String.valueOf(mblog_ret_num_norm),
				String.valueOf(mblog_cmt_num_norm),
				String.valueOf(mblog_like_num_norm),

				String.valueOf(mblog_hot_heat),
				String.valueOf(mblog_hot_heat_norm),
				String.valueOf(mblog_heat),
				String.valueOf(mblog_heat_norm),

				String.valueOf(mblog_click_pic_num),
				String.valueOf(mblog_click_video_num),
				String.valueOf(mblog_click_single_page_num),
				String.valueOf(mblog_click_follow_num),
				String.valueOf(mblog_click_article_num),
				String.valueOf(mblog_new_click_num),
				String.valueOf(mblog_click_num_rate),
				String.valueOf(mblog_group_click_num_rate),

				String.valueOf(mblog_click_pic_rate),
				String.valueOf(mblog_click_video_rate),
				String.valueOf(mblog_click_single_page_rate),
				String.valueOf(mblog_click_follow_rate),
				String.valueOf(mblog_click_article_rate),
				String.valueOf(mblog_hot_ret_rate),
				String.valueOf(mblog_hot_cmt_rate),
				String.valueOf(mblog_hot_like_rate),

				String.valueOf(mblog_group_click_pic_rate),
				String.valueOf(mblog_group_click_video_rate),
				String.valueOf(mblog_group_click_single_page_rate),
				String.valueOf(mblog_group_click_follow_rate),
				String.valueOf(mblog_group_click_article_rate),

				String.valueOf(mblog_real_expo_num),
				String.valueOf(mblog_real_group_expo_num),
				String.valueOf(mblog_real_click_rate),
				String.valueOf(mblog_real_interact_rate),
				String.valueOf(mblog_real_group_click_rate),
				String.valueOf(mblog_real_group_interact_rate),

				String.valueOf(mblog_real_click_pic_rate),
				String.valueOf(mblog_real_click_video_rate),
				String.valueOf(mblog_real_click_sing_page_rate),
				String.valueOf(mblog_real_click_follow_rate),
				String.valueOf(mblog_real_click_article_rate),
				String.valueOf(mblog_real_ret_rate),
				String.valueOf(mblog_real_cmt_rate),
				String.valueOf(mblog_real_like_rate),

				String.valueOf(mblog_real_group_click_pic_rate),
				String.valueOf(mblog_real_group_click_video_rate),
				String.valueOf(mblog_real_group_click_single_page_rate),
				String.valueOf(mblog_real_group_click_follow_rate),
				String.valueOf(mblog_real_group_click_article_rate),

				String.valueOf(mblog_real_read_duration),
				String.valueOf(mblog_real_read_uv),
				String.valueOf(mblog_read_duration_avg),

				String.valueOf(mblog_real_city_level_expo_num),
				String.valueOf(mblog_real_city_level_act_num),
				String.valueOf(mblog_real_city_level_interact_num),
				String.valueOf(mblog_real_city_level_click_rate),
				String.valueOf(mblog_real_city_level_interact_rate),

				String.valueOf(mblog_province_group_ctr),
				String.valueOf(mblog_province_group_click),
				String.valueOf(mblog_province_group_expo),

				// --- interact
				match_first_tag,
				match_second_tag,
				match_third_tag,

				match_first_tag_v2,
				match_second_tag_v2,
				match_third_tag_v2,

				match_first_long_tag,
				match_second_long_tag,
				match_third_long_tag,

				match_first_short_tag,
				match_second_short_tag,
				match_third_short_tag,

				String.valueOf(match_first_group_ctr),
				String.valueOf(match_second_group_ctr),
				String.valueOf(match_third_group_ctr),

				String.valueOf(match_first_group_v2_ctr),
				String.valueOf(match_second_group_v2_ctr),
				String.valueOf(match_third_group_v2_ctr),

				String.valueOf(match_first_group_long_ctr),
				String.valueOf(match_second_group_long_ctr),
				String.valueOf(match_third_group_long_ctr),

				String.valueOf(match_first_group_short_ctr),
				String.valueOf(match_second_group_short_ctr),
				String.valueOf(match_third_group_short_ctr),

				match_first_level_inte_weight,
				match_second_level_inte_weight,
				match_third_level_inte_weight,

				String.valueOf(match_first_tag_user_value),
				String.valueOf(match_second_tag_user_value),
				String.valueOf(match_third_tag_user_Value),
				String.valueOf(match_first_tag_mblog_value),
				String.valueOf(match_second_tag_mblog_value),
				String.valueOf(match_third_tag_mblog_value),

				String.valueOf(match_first_tag_user_value_v2),
				String.valueOf(match_second_tag_user_value_v2),
				String.valueOf(match_third_tag_user_Value_v2),
				String.valueOf(match_first_tag_mblog_value_v2),
				String.valueOf(match_second_tag_mblog_value_v2),
				String.valueOf(match_third_tag_mblog_value_v2),

				String.valueOf(match_first_tag_user_long_value),
				String.valueOf(match_second_tag_user_long_value),
				String.valueOf(match_third_tag_user_long_Value),
				String.valueOf(match_first_tag_mblog_long_value),
				String.valueOf(match_second_tag_mblog_long_value),
				String.valueOf(match_third_tag_mblog_long_value),

				String.valueOf(match_first_tag_user_short_value),
				String.valueOf(match_second_tag_user_short_value),
				String.valueOf(match_third_tag_user_short_Value),
				String.valueOf(match_first_tag_mblog_short_value),
				String.valueOf(match_second_tag_mblog_short_value),
				String.valueOf(match_third_tag_mblog_short_value),

				match_first_tag_ctrs,
				match_second_tag_ctrs,
				match_third_tag_ctrs,

				match_user_author_intimacy,

				String.valueOf(is_match_location),
				is_match_long_interest,
				is_match_short_interest,
				is_match_near_interest,
				is_match_instant_interest
		);
	}
}
