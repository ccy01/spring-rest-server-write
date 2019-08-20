package com.nikey.util;

import com.nikey.service.AbnormalDataService;
import com.nikey.service.GroupInfoService;
import com.nikey.service.HokoCapacityToDemandService;
import com.nikey.service.KilowService;
import com.nikey.service.Klm4134Service;
import com.nikey.service.MonitordataService;
import com.nikey.service.TransFormerInfoService;

public class ServiceHelper {

	/**
	 * service helper
	 */
	private AbnormalDataService abnormalDataService;
	private Klm4134Service klm4134Service;
	private GroupInfoService groupInfoService;
	private TransFormerInfoService transFormerInfoService;
	private MonitordataService monitordataService;
	private KilowService kilowService;
	private HokoCapacityToDemandService hokoCapacityToDemandService;

	/**
	 * singleton
	 */
	private static final ServiceHelper instance = new ServiceHelper();

	public static ServiceHelper instance() {
		return instance;
	}

	public KilowService getKilowService() {
		return kilowService;
	}

	public void setKilowService(KilowService kilowService) {
		this.kilowService = kilowService;
	}

	public TransFormerInfoService getTransFormerInfoService() {
		return transFormerInfoService;
	}

	public void setTransFormerInfoService(TransFormerInfoService transFormerInfoService) {
		this.transFormerInfoService = transFormerInfoService;
	}

	public MonitordataService getMonitordataService() {
		return monitordataService;
	}

	public void setMonitordataService(MonitordataService monitordataService) {
		this.monitordataService = monitordataService;
	}

	public GroupInfoService getGroupInfoService() {
		return groupInfoService;
	}

	public void setGroupInfoService(GroupInfoService groupInfoService) {
		this.groupInfoService = groupInfoService;
	}

	private ServiceHelper() {
	}

	public AbnormalDataService getAbnormalDataService() {
		return abnormalDataService;
	}

	public void setAbnormalDataService(AbnormalDataService abnormalDataService) {
		this.abnormalDataService = abnormalDataService;
	}

	public Klm4134Service getKlm4134Service() {
		return klm4134Service;
	}

	public void setKlm4134Service(Klm4134Service klm4134Service) {
		this.klm4134Service = klm4134Service;
	}

	public HokoCapacityToDemandService getHokoCapacityToDemandService() {
		return hokoCapacityToDemandService;
	}

	public void setHokoCapacityToDemandService(HokoCapacityToDemandService hokoCapacityToDemandService) {
		this.hokoCapacityToDemandService = hokoCapacityToDemandService;
	}

}
